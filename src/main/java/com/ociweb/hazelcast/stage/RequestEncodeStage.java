package com.ociweb.hazelcast.stage;

import com.ociweb.hazelcast.HazelcastConfigurator;
import com.ociweb.hazelcast.stage.util.LittleEndianByteHelpers;
import com.ociweb.pronghorn.pipe.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RequestEncodeStage extends PronghornStage {

    // temp
    private int iter = 1;

    private final Pipe   input;
    private final Pipe<RawDataSchema>[] outputs;
    private Pipe<RawDataSchema>[] indexedOutputs;

    private final HazelcastConfigurator config;
    private final int modValue;
    private FieldReferenceOffsetManager inputFrom;
    private int outputsRoundCursor = 0;
    private StringBuilder tempAppendable = new StringBuilder(256);

    private final static long ID_CORRELATIONID = 0x1ffff0;
    private final static long ID_PARTITIONHASH = 0x1fffef;
    private final static int  OFFSET_CORRELATIONID = 1;  // All messages must have these two positions.
    private final static int  OFFSET_PARTITIONHASH = 2;  // Note: the code assumes these two are first so starts fields at 3
    private final static byte BIT_FLAG_START = (byte)0x80;
    private final static byte BIT_FLAG_END = (byte)0x40;

    private final static Logger log = LoggerFactory.getLogger(RequestEncodeStage.class);

    // ToBeResolved: Is this the right place for the config?  Only used for HashKey (which may be sufficient to keep)
    public RequestEncodeStage(GraphManager graphManager, Pipe<HazelcastRequestsSchema> input, Pipe<RawDataSchema>[] outputs, HazelcastConfigurator config) {
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
            indexedOutputs[config.getHashKeyForRingId(outputs[i].id)] = outputs[i];
        }
        // TODO(cas): This is where the modValue will be set to reflect the number of machines in the cluster.
    }


    @Override
    public void run() {

        while (Pipe.hasContentToRead(input)) {
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
                if (outputs.length > 1) {
                    do {
                        if (--outputsRoundCursor < 0) {
                            outputsRoundCursor = outputs.length - 1;
                        }
                    } while (outputsRoundCursor != original && !Pipe.hasRoomForWrite(outputs[outputsRoundCursor]));

                    if (outputsRoundCursor == original) {
                        // no room was found
                        return;
                    }
                } else {
                    if (!Pipe.hasRoomForWrite(outputs[outputsRoundCursor])) {
                        return;
                    }
                }
                targetOutput = outputs[outputsRoundCursor];
            } else {
                targetOutput = outputs[hashCode % modValue];
                // Output Ring limits must have varLength > 18 to send header and make some progress >= 19
                // If the target pipe cannot take this message, then exit.
                // The invoking facility will be responsible for sending the message back to try later.
                // Note Well: One output message may only be a fragment of the full message to be sent.
                if (!Pipe.hasRoomForWrite(targetOutput)) {
                    return;
                }
            }

            // Add the message index to the output pipe as required by all messages.
            final int rawDataMessageSize = Pipe.addMsgIdx(targetOutput, RawDataSchema.MSG_CHUNKEDSTREAM_1);

            final int msgIdx = Pipe.takeMsgIdx(input);

            // FUTURE: This can be made faster with code generation for every message type.

            long inputMsgId = inputFrom.fieldIdScript[msgIdx];

            //gather all the destination variables
            int outputBytePos = Pipe.getBlobWorkingHeadPosition(targetOutput);
            final int startOutputBytePos = outputBytePos;
            byte[] outputByteBuffer = Pipe.byteBuffer(targetOutput);
            int outputByteMask = Pipe.blobMask(targetOutput);

  //          outputBytePos+=4;
            outputBytePos = encodeCorrelationIdAndPartitionHash(msgIdx, targetOutput, outputBytePos, outputByteBuffer, outputByteMask);

            System.err.println("msgIdx: " + msgIdx + ", inputMsgId: 0X" + Long.toHexString(inputMsgId));
            switch ((int) inputMsgId) {
                // CreateProxy
                case 0x05:
                    outputBytePos = encodeCreateProxy(outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // DestroyProxy
                case 0x06:
                    outputBytePos = encodeDestroyProxy(outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // GetPartitions
                case 0x08:
                    // No-op the only things being sent are the Message Index and the IDs and they are already in the pipe.
                    break;

                // Size
                case 0x0601:
                    outputBytePos = encodeSize(msgIdx, targetOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // Contains
                case 0x0602:
                    outputBytePos = encodeContains(outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // ContainsAll
                case 0x0603:
                    outputBytePos = encodeContainsAll(outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // Add
                case 0x0604:
                    outputBytePos = encodeAdd(outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // Remove
                case 0x0605:
                    encodeRemove(msgIdx, targetOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // AddAll
                case 0x0606:
                    encodeAddAll(msgIdx, targetOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // CompareAndRemoveAll
                case 0x0607:
                    encodeCompareAndRemoveAll(msgIdx, targetOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // CompareAndRetainAll
                case 0x0608:
                    encodeCompareAndRetainAll(msgIdx, targetOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // Clear
                case 0x0609:
                    encodeClear(msgIdx, targetOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // GetAll
                case 0x060a:
                    encodeGetAll(msgIdx, targetOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // AddListener
                case 0x060b:
                    encodeAddListener(msgIdx, targetOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // RemoveListener
                case 0x060c:
                    encodeRemoveListener(msgIdx, targetOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // IsEmpty
                case 0x060d:
                    encodeIsEmpty(msgIdx, targetOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                default:
                    System.out.println("RequestEncodeStage MsgTypeCase: Unknown inputMsgId: " + inputMsgId);
                    return;
            }

            int writeLen = outputBytePos - startOutputBytePos;
            Pipe.addAndGetBytesWorkingHeadPosition(targetOutput, writeLen);
            Pipe.addBytePosAndLenSpecial(targetOutput, startOutputBytePos, writeLen);
            Pipe.confirmLowLevelWrite(targetOutput, rawDataMessageSize);
            Pipe.publishWrites(targetOutput);
            Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIdx));
            Pipe.releaseReads(input);
        }
    }

    /*
        The following constructs are a bit confusing.  Why two sendBlobContents in a row?  There is one
        sendBlobContent for each piece of information being sent in the message.  For example, the size
        has only one piece of information, the name.  However CreateProxy has both name and serviceName,
        so there are two. To say it another way, there should be one sendBlobContent for every field in
        the message after the partitionHash.
     */
    private int encodeDestroyProxy(int outputBytePos, byte[] outputByteBuffer, int outputByteMask) {
        outputBytePos = sendBlobContent(outputBytePos, outputByteBuffer, outputByteMask);
        return sendBlobContent(outputBytePos, outputByteBuffer, outputByteMask);
    }

    private int encodeCreateProxy(int outputBytePos, byte[] outputByteBuffer, int outputByteMask) {
        outputBytePos = sendBlobContent(outputBytePos, outputByteBuffer, outputByteMask);
        return sendBlobContent(outputBytePos, outputByteBuffer, outputByteMask);
    }

    private int encodeSize(int msgIdx, Pipe<RawDataSchema> targetOutput, int outputBytePos, byte[] outputByteBuffer, int outputByteMask) {
        return sendBlobContent(outputBytePos, outputByteBuffer, outputByteMask);
    }

    private int encodeContains(int outputBytePos, byte[] outputByteBuffer, int outputByteMask) {
        outputBytePos = sendBlobContent(outputBytePos, outputByteBuffer, outputByteMask);
        return sendBlobContent(outputBytePos, outputByteBuffer, outputByteMask);
    }

    private int encodeContainsAll(int outputBytePos, byte[] outputByteBuffer, int outputByteMask) {
        outputBytePos = sendBlobContent(outputBytePos, outputByteBuffer, outputByteMask);
        return sendBlobContent(outputBytePos, outputByteBuffer, outputByteMask);
    }

    private int encodeAdd(int outputBytePos, byte[] outputByteBuffer, int outputByteMask) {
        outputBytePos = sendBlobContent(outputBytePos, outputByteBuffer, outputByteMask);
        return sendBlobContent(outputBytePos, outputByteBuffer, outputByteMask);
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

    private int sendBlobContent(int outputBytePos, byte[] outputByteBuffer, int outputByteMask) {
        int sourceMetaData = Pipe.takeRingByteMetaData(input);
        int sourceFieldLength = Pipe.takeRingByteLen(input);
        int sourceByteMask = Pipe.blobMask(input);
        byte[] sourceByteBuffer = Pipe.byteBackingArray(sourceMetaData, input);
        int sourceBytePosition = Pipe.bytePosition(sourceMetaData, input, sourceFieldLength);

        outputBytePos = LittleEndianByteHelpers.writeInt32(sourceFieldLength, outputBytePos, outputByteBuffer, outputByteMask);
        Pipe.copyBytesFromToRing(sourceByteBuffer, sourceBytePosition, sourceByteMask, outputByteBuffer, outputBytePos, outputByteMask, sourceFieldLength);
        outputBytePos += sourceFieldLength;
        return outputBytePos;
    }

    private int beginWriteToOutputPipe(int msgIdx, Pipe<RawDataSchema> targetOutput, int outputBytePos,
            byte[] outputByteBuffer, int outputByteMask, int correlationId, int partitionHash) {
        int size = inputFrom.fragScriptSize[msgIdx];
        int bytesCount = Pipe.peekInt(input, size - 2);
//        System.out.println("Total variable bytes to write:" + bytesCount + " not counting lengths");

        int maxBytesCount = bytesCount + (size * 2);                // rough estimate on the  high end
        if (maxBytesCount > (targetOutput.maxAvgVarLen - 4)) {      // use 4 because we never split a primitive field.
            int parts = 1 + (maxBytesCount / targetOutput.maxAvgVarLen);
            int limit = (maxBytesCount / parts);                    // TODO: must finish this split logic later.
        }

        // Hazelcast requires 5 byte length before the packet.  This value is NOT written here on the front of the
        // packet instead it is in the fixed length section,  On socket transmit it will be sent first.
        outputByteBuffer[outputByteMask & outputBytePos++] = 1;  //version 1 byte const
        outputByteBuffer[outputByteMask & outputBytePos++] = BIT_FLAG_START | BIT_FLAG_END;  //flags   1 byte  begin/end  zeros

        long msgId = inputFrom.fieldIdScript[msgIdx];
        outputByteBuffer[outputByteMask & outputBytePos++] = (byte) (0xFF & msgId); //type 2 bytes (this is the messageId)
        outputByteBuffer[outputByteMask & outputBytePos++] = (byte) (0xFF & (msgId >> 8));

        outputBytePos = LittleEndianByteHelpers.writeInt32(correlationId, outputBytePos, outputByteBuffer, outputByteMask);

        outputBytePos = LittleEndianByteHelpers.writeInt32(partitionHash, outputBytePos, outputByteBuffer, outputByteMask);

        outputByteBuffer[outputByteMask & outputBytePos++] = 18;  // 0x12  2 bytes for data offset
        outputByteBuffer[outputByteMask & outputBytePos++] = 0;
        return outputBytePos;
    }


    private int encodeCorrelationIdAndPartitionHash(int msgIdx, Pipe<RawDataSchema> targetOutput, int outputBytePos, byte[] outputByteBuffer, int outputByteMask) {
        int correlationId = Pipe.takeValue(input);
        int partitionHash = Pipe.takeValue(input);
        outputBytePos = beginWriteToOutputPipe(msgIdx, targetOutput, outputBytePos, outputByteBuffer, outputByteMask, correlationId, partitionHash);
        return outputBytePos;
    }


    private int writeUTFToByteBuffer(int bytePos, byte[] byteBuffer, int byteMask) {
        int len = tempAppendable.length();
        bytePos = LittleEndianByteHelpers.writeInt32(len, bytePos, byteBuffer, byteMask);
        byte[] source = tempAppendable.toString().getBytes();
        int c = 0;
        while (c < len) {
            bytePos = Pipe.encodeSingleChar((int) source[c++], byteBuffer, byteMask, bytePos);
        }
        return bytePos;
    }



    @Override
    public void shutdown() {
        System.err.println("RequestEncodeStage shutdown at system time: " + System.currentTimeMillis());
    }
}
