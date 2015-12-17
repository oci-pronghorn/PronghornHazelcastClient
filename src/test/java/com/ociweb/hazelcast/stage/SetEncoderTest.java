package com.ociweb.hazelcast.stage;

import com.ociweb.hazelcast.util.EncoderTestGenerator;
import com.ociweb.hazelcast.util.EncoderTestValidator;
import com.ociweb.hazelcast.util.ExpectedEncoderMessageBuilder;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.route.SplitterStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Test the Hazelcast Client Set method encoding
 */
public class SetEncoderTest {

    @Test
    public void setApiTest() {

        GraphManager gm = new GraphManager();

        // Create Generator Stage (from Pronghorn Pipes).
        PipeConfig<HazelcastRequestsSchema> hzReqConfig = new PipeConfig<>(HazelcastRequestsSchema.instance, 5, 1024);
        Pipe<HazelcastRequestsSchema> generatorPipe = new Pipe<>(hzReqConfig);

        long seed = 17L;
        int iterations = 11;

        Pipe<HazelcastRequestsSchema> pipeToEncoder = new Pipe<>(hzReqConfig.grow2x());
        new EncoderTestGenerator(gm, seed, iterations, pipeToEncoder);

        Pipe<HazelcastRequestsSchema> pipeToExpecteds = new Pipe<>(hzReqConfig.grow2x());
        new EncoderTestGenerator(gm, seed, iterations, pipeToExpecteds);

//        new ConsoleJSONDumpStage<>(gm, pipeToExpecteds);
//        new ConsoleJSONDumpStage<>(gm, pipeToEncoder, System.err);


        // Create Encoder w/Pipes.
        PipeConfig<RawDataSchema> rawBytes = new PipeConfig<>(RawDataSchema.instance, 5, 2048);
        Pipe<RawDataSchema>[] encoderToValidator = new Pipe[1];
        encoderToValidator[0] = new Pipe<>(rawBytes);
        new RequestEncodeStage(gm, pipeToEncoder, encoderToValidator, new Configurator()); // This is the class under test.

        // Create the class that builds the expected test values
        Pipe<RawDataSchema> expectedMessagesToValidatorPipe = new Pipe<>(rawBytes);
        new ExpectedEncoderMessageBuilder(gm, pipeToExpecteds, expectedMessagesToValidatorPipe);

        // Use these to see the results generated by the two receivers of the split stream.
//        new ConsoleJSONDumpStage<>(gm, expectedMessagesToValidatorPipe);
//        new ConsoleJSONDumpStage<>(gm, encoderToValidator[0], System.err);
        new EncoderTestValidator<>(gm, expectedMessagesToValidatorPipe, encoderToValidator[0]);

        MonitorConsoleStage.attach(gm);

        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        scheduler.startup();

        scheduler.awaitTermination(300, TimeUnit.SECONDS);
    }
}
