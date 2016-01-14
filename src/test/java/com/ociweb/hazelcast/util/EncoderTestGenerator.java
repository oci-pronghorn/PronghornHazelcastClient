package com.ociweb.hazelcast.util;

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
    private int iter = 1;

    public EncoderTestGenerator(GraphManager gm, long seed, int iterations, Pipe output) {
        super(gm, NONE, output);
        this.iterations = iterations;
        StreamingWriteVisitor visitor = new StreamingWriteVisitorGenerator(Pipe.from(output), new Random(seed),
            output.maxAvgVarLen>>3,  // Ensure there will be room for the maximum number of UTF8 chars.
            output.maxAvgVarLen>>1); //just use half
        this.writer = new StreamingVisitorWriter(output, visitor);
    }


    @Override
    public void startup() {
        writer.startup();
    }

    @Override
    public void run() {
        writer.run();
        iterations--;
        if (iterations == 0) {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            requestShutdown();
        }
    }

    @Override
    public void shutdown() {
        System.err.println("EncoderTestGenerator shutdown: " + System.currentTimeMillis());
        writer.shutdown();
    }

}
