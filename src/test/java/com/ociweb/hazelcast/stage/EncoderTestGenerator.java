package com.ociweb.hazelcast.stage;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.StreamingVisitorWriter;
import com.ociweb.pronghorn.pipe.stream.StreamingWriteVisitor;
import com.ociweb.pronghorn.pipe.stream.StreamingWriteVisitorGenerator;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import java.util.Random;

public class EncoderTestGenerator extends PronghornStage {

    private final StreamingVisitorWriter writer;
    private int iterations;

    public EncoderTestGenerator(GraphManager gm, long seed, int iterations, Pipe output) {
        super(gm, NONE, output);
        this.iterations = iterations;
        StreamingWriteVisitor visitor = new StreamingWriteVisitorGenerator(Pipe.from(output), new Random(seed),
            output.maxAvgVarLen>>3,  //room for UTF8
            output.maxAvgVarLen>>1); //just use half
        this.writer = new StreamingVisitorWriter(output, visitor);
    }


    @Override
    public void startup() {
        writer.startup();
    }

    @Override
    public void run() {
        if (iterations-- > 0) {
            writer.run();
        } else {
            requestShutdown();
        }
    }

    @Override
    public void shutdown() {
        writer.shutdown();
    }

}
