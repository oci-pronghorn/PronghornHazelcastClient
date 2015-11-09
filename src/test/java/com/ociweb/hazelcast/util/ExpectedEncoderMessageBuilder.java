package com.ociweb.hazelcast.util;

import com.ociweb.hazelcast.stage.HazelcastRequestsSchema;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitor;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitorMatcher;
import com.ociweb.pronghorn.pipe.stream.StreamingVisitorReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpectedEncoderMessageBuilder extends PronghornStage {

    private final StreamingVisitorReader reader;

    private final static Logger log = LoggerFactory.getLogger(ExpectedEncoderMessageBuilder.class);

    /*
     * All messages must have the following two positions as the first two messages.
     * Note well that the code assumes the CorrelationId and PartitionHash are the
     * first two fields, so the real message fields start at offset 3.
     */
    private final static int OFFSET_CORRELATIONID = 1;
    private final static int OFFSET_PARTITIONHASH = 2;

    private final static long ID_CORRELATIONID = 0x1ffff0;
    private final static long ID_PARTITIONHASH = 0x1fffef;

    public ExpectedEncoderMessageBuilder(GraphManager graphManager, Pipe<HazelcastRequestsSchema> input, Pipe<RawDataSchema> output) {
        super(graphManager, input, output);

        // Ensure all messages must have the 0x1ffff0 CorrelationID and 0x1fffef PartitionHash in the expected position.
        assert(expectedFieldPositions(Pipe.from(input))) : "The CorrelationId and PartitionHash must be in the first and second position for all messages";

        // TODO: Keep? This is not as necessary as it was pre-generics.
        assert(expectedFrom(output, FieldReferenceOffsetManager.RAW_BYTES)) : "Expected simple raw bytes for output.";

        StreamingReadVisitor visitor = new RequestEncoderTestVisitor(output);
        this.reader = new StreamingVisitorReader(input,  visitor);
    }

    private boolean expectedFieldPositions(FieldReferenceOffsetManager from) {
        int[] starts = from.messageStarts();
        int i = starts.length;
        while (--i >= 0) {
            int msgIdx = starts[i];
            if (ID_CORRELATIONID != from.fieldIdScript[msgIdx + OFFSET_CORRELATIONID]) {
                log.error("Expected correlation id as first field in message {} ", Long.toHexString(msgIdx));
                log.error("Found correlation id: " + from.fieldIdScript[msgIdx + OFFSET_CORRELATIONID]);
                log.error("Expected correlation id: " + ID_CORRELATIONID);
                log.error("Msg Number: " + i);
                log.error("MsgIdx: " + msgIdx);
                return false;
            }
            if (ID_PARTITIONHASH != from.fieldIdScript[msgIdx + OFFSET_PARTITIONHASH]) {
                log.error("Expected partition hash as second field in message {} ", Long.toHexString(msgIdx));
                return false;
            }
        }
        return true;
    }

    private boolean expectedFrom(Pipe<RawDataSchema> pipeToCheck, FieldReferenceOffsetManager rawBytes) {
        if (!Pipe.from(pipeToCheck).equals(rawBytes)) {
            return false;
        }
        return true;
    }

    @Override
    public void startup() {
        // Reorder the pipes so they line up with the hashed mod for easy sending of packets to the right node.
        /* For actual encoder only
            int i = outputs.length;
            indexedOutputs = new Pipe[i];
            while (--i >= 0) {
                indexedOutputs[config.getHashKeyForRingId(outputs[i].ringId)] = outputs[i];
            }
        */
        reader.startup();
    }

    @Override
    public void run() {
        reader.run();
    }

    @Override
    public void shutdown() {
        reader.shutdown();
    }

}
