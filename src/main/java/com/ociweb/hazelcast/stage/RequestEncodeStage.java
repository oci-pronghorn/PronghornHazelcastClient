package com.ociweb.hazelcast.stage;

import com.ociweb.pronghorn.pipe.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RequestEncodeStage extends PronghornStage {

    private final Pipe   input;
    private final Pipe[] outputs;
    private Pipe[] indexedOutputs;

    private final Configurator config;
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

        while (Pipe.hasContentToRead(input)) {
            // The hash code is always the second field.  Use it to figure out which pipe or pipes
            // will be used, then ensure there is enough room in the intended destinations.
            // If there isn't, return.
            int hashCode = Pipe.peekInt(input, 2);

            Pipe<RawDataSchema> destOutput;
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
                destOutput = outputs[outputsRoundCursor];
            } else {
                destOutput = outputs[hashCode % modValue];
                // Output Ring limits must have varLength > 18 to send header and make some progress >= 19
                // If the target pipe cannot take this message, then exit.
                // The invoking facility will be responsible for sending the message back to try later.
                // Note Well: One output message may only be a fragment of the full message to be sent.
                if (!Pipe.hasRoomForWrite(destOutput)) {
                    return;
                }
            }

            // Add the message index to the output pipe as required by all messages.
            final int rawDataMessageSize = Pipe.addMsgIdx(destOutput, RawDataSchema.MSG_CHUNKEDSTREAM_1);

            final int msgIdx = Pipe.takeMsgIdx(input);

            // FUTURE: This can be made faster with code generation for every message type.
            if (msgIdx > 5) {
                System.out.println("invalid msgIdx: " + msgIdx);
                return;
            }

            long inputMsgId = inputFrom.fieldIdScript[msgIdx];

            //gather all the destination variables
            int outputBytePos = Pipe.bytesWorkingHeadPosition(destOutput);
            final int startOutputBytePos = outputBytePos;
            byte[] outputByteBuffer = Pipe.byteBuffer(destOutput);
            int outputByteMask = Pipe.blobMask(destOutput);

            switch ((int) inputMsgId) {
                // Size
                case 0x0601:
                    System.out.println("encoder: encodeSize invocation bytePosBefore:" + outputBytePos);
                    outputBytePos = encodeSize(msgIdx, destOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    System.out.println("encoder: encodeSize invocation bytePosAfter:" + outputBytePos);
                    break;

                // Contains
                case 0x0602:
                    outputBytePos = encodeContains(msgIdx, destOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // ContainsAll
                case 0x0603:
                    outputBytePos = encodeContainsAll(msgIdx, destOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // Add
                case 0x0604:
                    encodeAdd(msgIdx, destOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // Remove
                case 0x0605:
                    encodeRemove(msgIdx, destOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // AddAll
                case 0x0606:
                    encodeAddAll(msgIdx, destOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // CompareAndRemoveAll
                case 0x0607:
                    encodeCompareAndRemoveAll(msgIdx, destOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // CompareAndRetainAll
                case 0x0608:
                    encodeCompareAndRetainAll(msgIdx, destOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // Clear
                case 0x0609:
                    encodeClear(msgIdx, destOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // GetAll
                case 0x060a:
                    encodeGetAll(msgIdx, destOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // AddListener
                case 0x060b:
                    encodeAddListener(msgIdx, destOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // RemoveListener
                case 0x060c:
                    encodeRemoveListener(msgIdx, destOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                // IsEmpty
                case 0x060d:
                    encodeIsEmpty(msgIdx, destOutput, outputBytePos, outputByteBuffer, outputByteMask);
                    break;

                default:
                    System.out.println("Not sure what this is. inputMsgId: " + inputMsgId);
                    return;
            }

            int writeLen = outputBytePos - startOutputBytePos;
            Pipe.addBytePosAndLenSpecial(destOutput, startOutputBytePos, writeLen);
            Pipe.confirmLowLevelWrite(destOutput, rawDataMessageSize);
            Pipe.publishWrites(destOutput);
            System.out.println("encoder: Wrote: " + writeLen + " bytes on encoder side.");
            Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIdx));
            Pipe.releaseReads(input);
            return;
        }
    }

    private int encodeSize(int msgIdx, Pipe<RawDataSchema> targetOutput, int outputBytePos, byte[] outputByteBuffer, int outputByteMask) {
        int correlationId = Pipe.takeValue(input);
        System.out.printf("encoder: correlationID is %d(%x)\n", correlationId, correlationId);
        int partitionHash = Pipe.takeValue(input);
        System.out.printf("encoder: partitionHash is %d(%x)\n", partitionHash, partitionHash);
        outputBytePos = beginWriteToOutputPipe(msgIdx, targetOutput, outputBytePos, outputByteBuffer, outputByteMask, correlationId, partitionHash);

        int sourceMetaData = Pipe.takeRingByteMetaData(input);
        int sourceFieldLength = Pipe.takeRingByteLen(input);
        System.out.println("encoder: encodeSize source field length is " + sourceFieldLength);
        int sourceByteMask = Pipe.blobMask(input);
        byte[] sourceByteBuffer = Pipe.byteBackingArray(sourceMetaData, input);
        int sourceBytePosition = Pipe.bytePosition(sourceMetaData, input, sourceFieldLength);

        outputBytePos = writeInt32(sourceFieldLength, outputBytePos, outputByteBuffer, outputByteMask);
        Pipe.copyBytesFromToRing(sourceByteBuffer, sourceBytePosition, sourceByteMask, outputByteBuffer, outputBytePos, outputByteMask, sourceFieldLength);
        return outputBytePos + sourceFieldLength;
    }

    private int encodeContains(int msgIdx, Pipe targetOutput, int bytePos, byte[] byteBuffer, int byteMask) {
        /*
        int correlationId = PipeReader.readInt(input, HazelcastRequestsSchema.MSG_CONTAINS_1538_FIELD_CORRELATIONID_2097136);
        int partitionHash = PipeReader.readInt(input, HazelcastRequestsSchema.MSG_CONTAINS_1538_FIELD_PARTITIONHASH_2097135);
        bytePos = beginWriteToOutputPipe(msgIdx, targetOutput, bytePos, byteBuffer, byteMask, correlationId, partitionHash);
        tempAppendable.setLength(0);
        PipeReader.readUTF8(input, HazelcastRequestsSchema.MSG_CONTAINS_1538_FIELD_NAME_458497, tempAppendable);
        bytePos = writeUTFToByteBuffer(bytePos, byteBuffer, byteMask);
        int len = PipeReader.readBytes(input, HazelcastRequestsSchema.MSG_CONTAINS_1538_FIELD_VALUE_458498, byteBuffer, bytePos, byteMask);
        return bytePos + len;
        */
        return 0;
    }

    private int encodeContainsAll(int msgIdx, Pipe targetOutput, int bytePos, byte[] byteBuffer, int byteMask) {
        /*
        int correlationId = PipeReader.readInt(input, HazelcastRequestsSchema.MSG_CONTAINSALL_1539_FIELD_CORRELATIONID_2097136);
        int partitionHash = PipeReader.readInt(input, HazelcastRequestsSchema.MSG_CONTAINSALL_1539_FIELD_PARTITIONHASH_2097135);
        bytePos = beginWriteToOutputPipe(msgIdx, targetOutput, bytePos, byteBuffer, byteMask, correlationId, partitionHash);
        tempAppendable.setLength(0);
        PipeReader.readUTF8(input, HazelcastRequestsSchema.MSG_CONTAINSALL_1539_FIELD_NAME_458497, tempAppendable);
        bytePos = writeUTFToByteBuffer(bytePos, byteBuffer, byteMask);
        int len = PipeReader.readBytes(input, HazelcastRequestsSchema.MSG_CONTAINSALL_1539_FIELD_VALUESET_458499, byteBuffer, bytePos, byteMask);

        return bytePos + len;
        */
        return 0;
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


    private int beginWriteToOutputPipe(int msgIdx, Pipe<RawDataSchema> targetOutput, int outputBytePos,
            byte[] outputByteBuffer, int outputByteMask, int correlationId, int partitionHash) {
        int size = inputFrom.fragScriptSize[msgIdx];
        int bytesCount = Pipe.peekInt(input, size - 2);
        System.out.println("Total variable bytes to write:" + bytesCount + " not counting lengths");

        int maxBytesCount = bytesCount + (size * 2);                // rough estimate on the  high end
        if (maxBytesCount > (targetOutput.maxAvgVarLen - 4)) {      // use 4 because we never split a primitive field.
            int parts = 1 + (maxBytesCount / targetOutput.maxAvgVarLen);
            int limit = (maxBytesCount / parts);                    // TODO: must finish this split logic later.
        }

        // Hazelcast requires 4 byte length before the packet.  This value is NOT written here on the front of the
        // packet instead it is in the fixed length section,  On socket transmit it will be sent first.
        outputByteBuffer[outputByteMask & outputBytePos++] = 1;  //version 1 byte const
        outputByteBuffer[outputByteMask & outputBytePos++] = BIT_FLAG_START | BIT_FLAG_END;  //flags   1 byte  begin/end  zeros

        long msgId = inputFrom.fieldIdScript[msgIdx];
        outputByteBuffer[outputByteMask & outputBytePos++] = (byte) (0xFF & msgId); //type 2 bytes (this is the messageId)
        outputByteBuffer[outputByteMask & outputBytePos++] = (byte) (0xFF & (msgId >> 8));

        outputBytePos = writeInt32(correlationId, outputBytePos, outputByteBuffer, outputByteMask);

        outputBytePos = writeInt32(partitionHash, outputBytePos, outputByteBuffer, outputByteMask);

        outputByteBuffer[outputByteMask & outputBytePos++] = 18;  // 0x12  2 bytes for data offset
        outputByteBuffer[outputByteMask & outputBytePos++] = 0;
        return outputBytePos;
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
