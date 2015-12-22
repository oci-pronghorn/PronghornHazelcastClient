package com.ociweb.hazelcast.stage;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * This class reads a response from the Hazelcast cluster in RawDataFormat. It decodes the response into a Correlation ID and
 * a HazelcastResponse and invokes the Callback associated with the Correlation ID.
 */

public class RequestDecodeStage extends PronghornStage {
    @SuppressWarnings("unchecked")
    public RequestDecodeStage(GraphManager gm, Pipe[] inputFromConnection, HazelcastConfigurator configurator) {
        super(gm, inputFromConnection, (Pipe)null);
    }

    @Override
    public void startup() {
    }

    @Override
    public void run() {
    }

    @Override
    public void shutdown() {
    }
}
