package com.ociweb.hazelcast.stage;

import static com.ociweb.pronghorn.pipe.Pipe.byteBackingArray;
import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RequestEncodeStage extends PronghornStage {

    private final Pipe input;
    private final Pipe[] outputs;
    private final Configurator config;
    private Pipe[] indexedOutputs;
    private FieldReferenceOffsetManager inputFrom;
    private final int msgSize;
    private final int modValue;
    private int outputsRoundCursor = 0;
    private int splitCursorPos = -1; //which field are we split on
    private int splitBytesPos = -1; //where in the byte buffer are split

    private final static long ID_CORRELATIONID = 0x1ffff0;
    private final static long ID_PARTITIONHASH = 0x1fffef;
    private final static int OFFSET_CORRELATIONID = 1;//ALL MESSAGES MUST HAVE THESE TWO POSITIONS.
    private final static int OFFSET_PARTITIONHASH = 2;//NOTE:CODE ASSUMES THESE TWO ARE FIRST SO STARTS FIELDS AT 3
    private final static byte BIT_FLAG_START = (byte)0x80;
    private final static byte BIT_FLAG_END = (byte)0x40;

    private final static Logger log = LoggerFactory.getLogger(RequestEncodeStage.class);


    protected RequestEncodeStage(GraphManager graphManager, Pipe input, Pipe[] outputs, Configurator config) {
        super(graphManager, input, outputs);
        this.input = input;
        this.outputs = outputs;
        this.config = config;
        // FIXME: Put a real value here -- ditto for ExpectedCreator stage
        this.modValue = 1;
        this.inputFrom = Pipe.from(input);

        //assert that all message have the 0x1ffff0 CorrelationID and 0x1fffef PartitionHash are in the expected position.
        assert(expectedFieldPositions(inputFrom)) : "The CorrelationId and PartitionHash must be in the first and second position for all messages";

        assert(expectedFrom(outputs, FieldReferenceOffsetManager.RAW_BYTES)) : "Expected simple raw bytes for output.";
        this.msgSize = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0]; //we only send one kind of message (packets to be sent)
    }

    private boolean expectedFieldPositions(FieldReferenceOffsetManager from) {
           int[] starts = from.messageStarts();
           int i = starts.length;
           while (--i>=0) {
               int msgIdx = starts[i];
               if (ID_CORRELATIONID != from.fieldIdScript[msgIdx+OFFSET_CORRELATIONID]) {
                   // TODO -- clean up the logging messages
                   log.error("expected correlation id as first field in message {} ",Long.toHexString(msgIdx));
                   log.error("Found correlation id: " + from.fieldIdScript[msgIdx+OFFSET_CORRELATIONID]);
                   log.error("Expected correlation id: " + ID_CORRELATIONID);
                   log.error("i: " + i);
                   log.error("msgIdx: " + msgIdx);
                   return false;
               }
               if (ID_PARTITIONHASH != from.fieldIdScript[msgIdx+OFFSET_PARTITIONHASH]) {
                   log.error("expected partition hash as second field in message {} ",Long.toHexString(msgIdx));
                   return false;
               }
           }
           return true;
    }

    private boolean expectedFrom(Pipe[] toCheck, FieldReferenceOffsetManager rawBytes) {
        int i = toCheck.length;
        while (--i >= 0) {
            if (!Pipe.from(toCheck[i]).equals(rawBytes)) {
                return false;
            }
        }
        return true;
    }


    @Override
    public void startup() {
        // Reorder the pipes so they line up with the hashed mod for easy sending of packets to the right node.
        int i = outputs.length;
        indexedOutputs = new Pipe[i];
        while (--i >= 0) {
            indexedOutputs[config.getHashKeyForRingId(outputs[i].ringId)] = outputs[i];
        }
        // TODO(nrt): review the FROM and build an index rule for where to find the key at runtime for every message
        // TODO(cas): The modValue will need to be set here to reflect the number of machines in the cluster.
    }


    @Override
    public void run() {

        while (Pipe.hasContentToRead(input, 1)) {

            // The hash code is always the second field.  Take a peek and figure out which pipe or pipes will be used
            // then check for enough room in the intended destinations.  If there isn't, return.
            int hashCode = Pipe.peekInt(input, 2);

            Pipe targetOutput;
            if (hashCode < 0) {
                // This is a command that does not have a particular partition, but rather is applicable to all the
                // machines in a cluster.  Consequently, all the output queues need to be checked for room.
                // This is done in round robin, skipping any that have a backed up queue.
                // TODO: Not doing this for now, but I don't think this code means what I think it means...
                final int original = outputsRoundCursor;
                do {
                    if (--outputsRoundCursor < 0) {
                        outputsRoundCursor = outputs.length - 1;
                    }
                } while (outputsRoundCursor != original && !Pipe.roomToLowLevelWrite(outputs[outputsRoundCursor], msgSize));

                if (outputsRoundCursor == original) {
                    // no room was found
                    return;
                }
                targetOutput = outputs[outputsRoundCursor];
            } else {
                targetOutput = outputs[hashCode % modValue];
                // If the target pipe can not take this message, then exit.  The invoking facility will be responsible
                // for sending the message back in to try in a later time slot.
                // Note Well: One output message may only be a fragment of the full message to be sent.
                if (!Pipe.roomToLowLevelWrite(targetOutput, msgSize)) {
                    return;
                }
            }

            //output Ring limits must have varLength > 18  to send header and make some progress. >=19

            int msgIdx = Pipe.takeMsgIdx(input);
            int correlationId = Pipe.takeValue(input); //this is position 1
            int partitionHash = Pipe.takeValue(input); //this is position 2
            //the remaining take field operators are below in the field loop


            //TODO: this can be made faster with code generation for every message type, someday in the future...
            int size = inputFrom.fragScriptSize[msgIdx];
            long msgId = inputFrom.fieldIdScript[msgIdx];

            int bytesCount = Pipe.peekInt(input, size-2);
            System.err.println("total bytes to write:"+bytesCount+" not counting lengths");
            int maxBytesCount = bytesCount + (size*2); //rough estimate on the  high end

            if (maxBytesCount > (targetOutput.maxAvgVarLen-4)) { //4 because we never split a primitive field.

                int parts = 1 + ( maxBytesCount / targetOutput.maxAvgVarLen);

                int limit = (maxBytesCount / parts); //TODO: must finish this split logic later.

            }

            //gather all the destination variables
            int bytePos = Pipe.bytesWorkingHeadPosition(targetOutput);
            final int startBytePos = bytePos;
            byte[] byteBuffer = Pipe.byteBuffer(targetOutput);
            int byteMask = Pipe.blobMask(targetOutput);

            //Hazelcast requires 4 byte length before the packet.  This value is
            //NOT written here on the front of the packet instead it is in the
            //fixed length section,  On socket xmit it will be sent first.

            byteBuffer[byteMask & bytePos++] = 1;  //version 1 byte const
            byteBuffer[byteMask & bytePos++] = BIT_FLAG_START | BIT_FLAG_END;  //flags   1 byte  begin/end  zeros

            byteBuffer[byteMask & bytePos++] = (byte)(0xFF&msgId); //type 2 bytes (this is the messageId)
            byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(msgId>>8));

            bytePos = writeInt32(correlationId, bytePos, byteBuffer, byteMask);
            bytePos = writeInt32(partitionHash, bytePos, byteBuffer, byteMask);

            byteBuffer[byteMask & bytePos++] = 13;  //13  2 bytes for data offset
            byteBuffer[byteMask & bytePos++] = 0;

            bytePos = writeAllFields(msgIdx, size, bytePos, byteBuffer, byteMask); ///TODO: need to add split and continue logic

            //done populate of byte buffer, now set length
            Pipe.addBytePosAndLenSpecial(targetOutput, startBytePos, bytePos-startBytePos);

        }
    }

    private int writeInt32(int value, int bytePos, byte[] byteBuffer, int byteMask) {
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>8));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>16));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>24));
        return bytePos;
    }

    private int writeAllFields(int msgIdx, int size, int bytePos, byte[] byteBuffer, int byteMask) {
        int i = 3;//NOTE: we assume that we have already read the correlation and partition
        while (i<size) {
            int idx = msgIdx+i;
            int token = inputFrom.tokens[idx];
            int type = TokenBuilder.extractType(token);

            switch(type) {
                case TypeMask.IntegerSigned:
                case TypeMask.IntegerSignedOptional:
                case TypeMask.IntegerUnsigned:
                case TypeMask.IntegerUnsignedOptional:
                    bytePos = writeInt32(Pipe.takeValue(input), bytePos, byteBuffer, byteMask);
                break;
                case TypeMask.LongSigned:
                case TypeMask.LongSignedOptional:
                case TypeMask.LongUnsigned:
                case TypeMask.LongUnsignedOptional:

                    long int64Value = Pipe.takeLong(input);//for int64

                    byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(int64Value));
                    byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(int64Value>>8));
                    byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(int64Value>>16));
                    byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(int64Value>>24));

                    byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(int64Value>>32));
                    byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(int64Value>>40));
                    byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(int64Value>>48));
                    byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(int64Value>>56));

                break;
                case TypeMask.TextASCII:
                case TypeMask.TextASCIIOptional:
                    throw new UnsupportedOperationException("All text for Hazelcast MUST be UTF8 encoded not ASCII");
                case TypeMask.ByteArray:
                case TypeMask.ByteArrayOptional:
                case TypeMask.TextUTF8:
                case TypeMask.TextUTF8Optional:

                    int meta = Pipe.takeRingByteMetaData(input); //for string and byte array
                    int len = Pipe.takeRingByteLen(input);

                    bytePos = writeInt32(len, bytePos, byteBuffer, byteMask);

                    Pipe.copyBytesFromToRing(byteBuffer, bytePos, byteMask,
                            byteBackingArray(meta, input), bytePosition(meta, input, len), Pipe.blobMask(input), len);
                    bytePos += len;

                break;
                case TypeMask.Group:
                    break;
                default:
                    throw new UnsupportedOperationException("unknown type "+TokenBuilder.tokenToString(token));

            }
            i += TypeMask.scriptTokenSize[type];
        }
        return bytePos;
    }
}
