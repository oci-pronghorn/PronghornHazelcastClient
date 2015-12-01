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
        PipeConfig<HazelcastRequestsSchema> hzReqConfig = new PipeConfig<>(HazelcastRequestsSchema.instance, 5, 256);
        Pipe<HazelcastRequestsSchema> generatorPipe = new Pipe<>(hzReqConfig);

        long seed = 43L;
        int iterations = 1;
        new EncoderTestGenerator(gm, seed, iterations, generatorPipe);

        // Create Splitter w/Pipes.
        Pipe<HazelcastRequestsSchema> pipeToExpecteds = new Pipe<>(hzReqConfig.grow2x());
        Pipe<HazelcastRequestsSchema> pipeToEncoder = new Pipe<>(hzReqConfig.grow2x());
        new SplitterStage<>(gm, generatorPipe, pipeToExpecteds, pipeToEncoder);

        // Create Encoder w/Pipes.
        PipeConfig<RawDataSchema> rawBytes = new PipeConfig<>(RawDataSchema.instance, 5, 512);
        Pipe<RawDataSchema>[] encoderToValidator = new Pipe[1];
        encoderToValidator[0] = new Pipe<>(rawBytes);
        // RequestEncodeStage is the class under test.
        new RequestEncodeStage(gm, pipeToEncoder, encoderToValidator, new Configurator());

        // Create the class to build the expected test values
        Pipe<RawDataSchema> expectedMessagesToValidatorPipe = new Pipe<>(rawBytes);
        new ExpectedEncoderMessageBuilder(gm, pipeToExpecteds, expectedMessagesToValidatorPipe);

        new EncoderTestValidator<>(gm, expectedMessagesToValidatorPipe, encoderToValidator[0]);

        MonitorConsoleStage.attach(gm);

        System.out.println("running test");
        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        scheduler.startup();

        scheduler.awaitTermination(30, TimeUnit.SECONDS);
        System.out.println("finished running test");
    }
}