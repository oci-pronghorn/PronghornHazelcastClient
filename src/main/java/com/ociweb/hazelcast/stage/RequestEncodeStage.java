package com.ociweb.hazelcast.stage;

import com.ociweb.pronghorn.pipe.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RequestEncodeStage extends PronghornStage {

    private final Pipe input;
    private final Pipe[] outputs;
    private final Configurator config;
    private Pipe[] indexedOutputs;
    private FieldReferenceOffsetManager inputFrom;
    private final int modValue;
    private int outputsRoundCursor = 0;
    private StringBuilder tempAppendable = new StringBuilder(256);

    private final static long ID_CORRELATIONID = 0x1ffff0;
    private final static long ID_PARTITIONHASH = 0x1fffef;
    private final static int OFFSET_CORRELATIONID = 1;//ALL MESSAGES MUST HAVE THESE TWO POSITIONS.
    private final static int OFFSET_PARTITIONHASH = 2;//NOTE:CODE ASSUMES THESE TWO ARE FIRST SO STARTS FIELDS AT 3
    private final static byte BIT_FLAG_START = (byte)0x80;
    private final static byte BIT_FLAG_END = (byte)0x40;

    private final static Logger log = LoggerFactory.getLogger(RequestEncodeStage.class);


    // ToBeResolved: Is this the right place for the config?  Only used for HashKey (which may be sufficient to keep)
    protected RequestEncodeStage(GraphManager graphManager, Pipe input, Pipe[] outputs, Configurator config) {
        super(graphManager, input, outputs);
        this.input = input;
        this.outputs = outputs;
        this.config = config;

        // FIXME: Put a real value here -- ditto for ExpectedCreator stage
        this.modValue = 1;
        this.inputFrom = Pipe.from(input);
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
        // TODO(cas): This is where the modValue will be set to reflect the number of machines in the cluster.
    }

    @Override
    public void run() {

        while (PipeReader.tryReadFragment(input)) {
            // The hash code is always the second field.  Use it to figure out which pipe or pipes
            // will be used, then ensure there is enough room in the intended destinations.
            // If there isn't, return.
            int hashCode = Pipe.peekInt(input, 2);

            Pipe<RawDataSchema> targetOutput;
            if (hashCode < 0) {
                // Deal with a command that does not have a particular partition, but rather is applicable to all the
                // machines in a cluster.  Check all the output queues to ensure there is room.
                // Check in round robin fashion skipping any that have a backed up queue.
                // TOBERESOLVED: Implemented commands are not doing this, but I don't think this code
                // matches the comments... Rather than all, seems to ensure there is at least one
                final int original = outputsRoundCursor;
                do {
                    if (--outputsRoundCursor < 0) {
                        outputsRoundCursor = outputs.length - 1;
                    }
                } while (outputsRoundCursor != original &&
                    !Pipe.hasRoomForWrite(outputs[outputsRoundCursor]));

                if (outputsRoundCursor == original) {
                    // no room was found
                    return;
                }
                targetOutput = outputs[outputsRoundCursor];
            } else {
                targetOutput = outputs[hashCode % modValue];
                // If the target pipe cannot take this message, then exit.
                // The invoking facility will be responsible for sending the message back to try later.
                // Note Well: One output message may only be a fragment of the full message to be sent.
                if (!Pipe.hasRoomForWrite(targetOutput)) {
                    return;
                }
            }

            // output Ring limits must have varLength > 18 to send header and make some progress >= 19
            int msgIdx = Pipe.takeMsgIdx(input);

            // FUTURE: This can be made faster with code generation for every message type.
            // The remaining take field operators are below in the field loop
            if (msgIdx > 5) {
                System.out.println("invalid msgIdx: " + msgIdx);
                return;
            }
            long msgId = inputFrom.fieldIdScript[msgIdx];

            //gather all the destination variables
            int bytePos = Pipe.bytesWorkingHeadPosition(targetOutput);
            final int startBytePos = bytePos;
            byte[] byteBuffer = Pipe.byteBuffer(targetOutput);
            int byteMask = Pipe.blobMask(targetOutput);

            switch ((int) msgId) {
                // Size
                case 0x0601:
                    bytePos = encodeSize(msgIdx, targetOutput, bytePos, byteBuffer, byteMask);
                    break;

                // Contains
                case 0x0602:
                    bytePos = encodeContains(msgIdx, targetOutput, bytePos, byteBuffer, byteMask);
                    break;

                // ContainsAll
                case 0x0603:
                    encodeContainsAll(msgIdx, targetOutput, bytePos, byteBuffer, byteMask);
                    break;

                // Add
                case 0x0604:
                    encodeAdd(msgIdx, targetOutput, bytePos, byteBuffer, byteMask);
                    break;

                // Remove
                case 0x0605:
                    encodeRemove(msgIdx, targetOutput, bytePos, byteBuffer, byteMask);
                    break;

                // AddAll
                case 0x0606:
                    encodeAddAll(msgIdx, targetOutput, bytePos, byteBuffer, byteMask);
                    break;

                // CompareAndRemoveAll
                case 0x0607:
                    encodeCompareAndRemoveAll(msgIdx, targetOutput, bytePos, byteBuffer, byteMask);
                    break;

                // CompareAndRetainAll
                case 0x0608:
                    encodeCompareAndRetainAll(msgIdx, targetOutput, bytePos, byteBuffer, byteMask);
                    break;

                // Clear
                case 0x0609:
                    encodeClear(msgIdx, targetOutput, bytePos, byteBuffer, byteMask);
                    break;

                // GetAll
                case 0x060a:
                    encodeGetAll(msgIdx, targetOutput, bytePos, byteBuffer, byteMask);
                    break;

                // AddListener
                case 0x060b:
                    encodeAddListener(msgIdx, targetOutput, bytePos, byteBuffer, byteMask);
                    break;

                // RemoveListener
                case 0x060c:
                    encodeRemoveListener(msgIdx, targetOutput, bytePos, byteBuffer, byteMask);
                    break;

                // IsEmpty
                case 0x060d:
                    encodeIsEmpty(msgIdx, targetOutput, bytePos, byteBuffer, byteMask);
                    break;

                default:
                    System.out.println("Not sure what this is. msgId: " + msgId);
                    return;
            }
            finishWriteToOutputPipe(targetOutput, startBytePos, bytePos - startBytePos);
            PipeReader.releaseReadLock(input);
            return;
        }
    }

    private int encodeSize(int msgIdx, Pipe<RawDataSchema> targetOutput, int bytePos, byte[] byteBuffer, int byteMask) {
        int correlationId = PipeReader.readInt(input, HazelcastRequestsSchema.MSG_SIZE_1537_FIELD_CORRELATIONID_2097136);
        int partitionHash = PipeReader.readInt(input, HazelcastRequestsSchema.MSG_SIZE_1537_FIELD_PARTITIONHASH_2097135);
        bytePos = beginWriteToOutputPipe(msgIdx, targetOutput, bytePos, byteBuffer, byteMask, correlationId, partitionHash);
        tempAppendable.setLength(0);
        PipeReader.readUTF8(input, HazelcastRequestsSchema.MSG_SIZE_1537_FIELD_NAME_458497, tempAppendable);
        return writeUTFToByteBuffer(bytePos, byteBuffer, byteMask);
    }

    private int encodeContains(int msgIdx, Pipe targetOutput, int bytePos, byte[] byteBuffer, int byteMask) {
        int correlationId = PipeReader.readInt(input, HazelcastRequestsSchema.MSG_CONTAINS_1538_FIELD_CORRELATIONID_2097136);
        int partitionHash = PipeReader.readInt(input, HazelcastRequestsSchema.MSG_CONTAINS_1538_FIELD_PARTITIONHASH_2097135);
        bytePos = beginWriteToOutputPipe(msgIdx, targetOutput, bytePos, byteBuffer, byteMask, correlationId, partitionHash);
        // LEFT OFF HERE -- -just have the PipeReader put the bytes directly into the buffer area
        // PipeReader.readBytes(input, HazelcastRequestsSchema.MSG,
        return bytePos;
    }

    private void encodeContainsAll(int msgIdx, Pipe targetOutput, int bytePos, byte[] byteBuffer, int byteMask) {
    }

    private void encodeAdd(int msgIdx, Pipe targetOutput, int bytePos, byte[] byteBuffer, int byteMask) {
    }

    private void encodeRemove(int msgIdx, Pipe targetOutput, int bytePos, byte[] byteBuffer, int byteMask) {
    }

    private void encodeAddAll(int msgIdx, Pipe targetOutput, int bytePos, byte[] byteBuffer, int byteMask) {
    }

    private void encodeCompareAndRemoveAll(int msgIdx, Pipe targetOutput, int bytePos, byte[] byteBuffer, int byteMask) {
    }

    private void encodeCompareAndRetainAll(int msgIdx, Pipe targetOutput, int bytePos, byte[] byteBuffer, int byteMask) {
    }

    private void encodeClear(int msgIdx, Pipe targetOutput, int bytePos, byte[] byteBuffer, int byteMask) {
    }

    private void encodeGetAll(int msgIdx, Pipe targetOutput, int bytePos, byte[] byteBuffer, int byteMask) {
    }

    private void encodeAddListener(int msgIdx, Pipe targetOutput, int bytePos, byte[] byteBuffer, int byteMask) {
    }

    private void encodeRemoveListener(int msgIdx, Pipe targetOutput, int bytePos, byte[] byteBuffer, int byteMask) {
    }

    private void encodeIsEmpty(int msgIdx, Pipe targetOutput, int bytePos, byte[] byteBuffer, int byteMask) {
    }


    private int beginWriteToOutputPipe(int msgIdx, Pipe<RawDataSchema> targetOutput, int bytePos, byte[] byteBuffer,
            int byteMask, int correlationId, int partitionHash) {
        int size = inputFrom.fragScriptSize[msgIdx];

        int bytesCount = Pipe.peekInt(input, size - 2);
        System.err.println("total bytes to write:" + bytesCount + " not counting lengths");

        int maxBytesCount = bytesCount + (size * 2);                // rough estimate on the  high end
        if (maxBytesCount > (targetOutput.maxAvgVarLen - 4)) {      // use 4 because we never split a primitive field.
            int parts = 1 + (maxBytesCount / targetOutput.maxAvgVarLen);
            int limit = (maxBytesCount / parts);                    // TODO: must finish this split logic later.
        }

        //Hazelcast requires 4 byte length before the packet.  This value is
        //NOT written here on the front of the packet instead it is in the
        //fixed length section,  On socket xmit it will be sent first.
        byteBuffer[byteMask & bytePos++] = 1;  //version 1 byte const
        byteBuffer[byteMask & bytePos++] = BIT_FLAG_START | BIT_FLAG_END;  //flags   1 byte  begin/end  zeros

        long msgId = 1537;
        byteBuffer[byteMask & bytePos++] = (byte) (0xFF & msgId); //type 2 bytes (this is the messageId)
        byteBuffer[byteMask & bytePos++] = (byte) (0xFF & (msgId >> 8));

        bytePos = writeInt32(correlationId, bytePos, byteBuffer, byteMask);

        bytePos = writeInt32(partitionHash, bytePos, byteBuffer, byteMask);

        byteBuffer[byteMask & bytePos++] = 19;  //13  2 bytes for data offset
        byteBuffer[byteMask & bytePos++] = 0;
        return bytePos;
    }

    private void finishWriteToOutputPipe(Pipe<RawDataSchema> targetOutput, int startBytePos, int len) {
        //done populate of byte buffer, now set length
        Pipe.addBytePosAndLenSpecial(targetOutput, startBytePos, len);
        Pipe.publishWrites(targetOutput);
        PipeReader.releaseReadLock(input);
        return;
    }

    private int writeUTFToByteBuffer(int bytePos, byte[] byteBuffer, int byteMask) {
        int len = tempAppendable.length();
        bytePos = writeInt32(len, bytePos, byteBuffer, byteMask);
        byte[] source = tempAppendable.toString().getBytes();
        int c = 0;
        while (c < len) {
            bytePos = Pipe.encodeSingleChar((int) source[c++], byteBuffer, byteMask, bytePos);
        }
        return bytePos;
    }

    private int writeInt32(int value, int bytePos, byte[] byteBuffer, int byteMask) {
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>8));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>16));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>24));
        return bytePos;
    }

}
