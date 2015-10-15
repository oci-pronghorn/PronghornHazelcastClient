package com.ociweb.hazelcast.impl;

import static org.junit.Assert.fail;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.route.SplitterStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.TestGenerator;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Test the Hazelcast Client Set method encoding
 */
public class SetEncoderTest {

    long seed = 42L;
    long iterations = 1000;

    @Test
    public void addTest() {


        GraphManager gm = new GraphManager();
        //

        // Create Generator Stage (from Pronghorn Pipes)
//        TestGenerator generator = new TestGenerator(gm, seed, iterations, output);

        // Create Splitter
//            Pipe<RawDataSchema> inputPipe = new Pipe<RawDataSchema>(config);
//            new SplitterStage(gm, inputPipe, midCheckPipe, outputPipe);

        // Create Encoder
//        RequestEncodeStage res = new RequestEncodeStage(gm, input, outputs, config);

        // Create Mapper (from Pipes)

        // Create Comparator (from Pipes?)

        // Create SetRequestEncoder Validator


// What's my schema?            PipeConfig<RawDataSchema> config = new PipeConfig<RawDataSchema>(RawDataSchema.instance, 10, 4096);


        // ConsoleStage cs = new ConsoleStage(gm, midCheckPipe);


        //           MonitorConsoleStage.attach(gm);

        System.out.println("running test");
//        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
//        scheduler.startup();

//        scheduler.awaitTermination(3, TimeUnit.SECONDS);
        System.out.println("finished running test");

        //when done check the captured bytes from teh middle to ensure they match
        //          assertArrayEquals(rawData, outputStream.toByteArray());

        //when done read the file from disk one more time and confirm its the same
        fail("No stages added to GM");
    }
}
