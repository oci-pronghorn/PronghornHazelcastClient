package com.ociweb.hazelcast.stage;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.route.SplitterStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.TestGenerator;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;


/**
 * Test the Hazelcast Client Set method encoding
 */
public class SetEncoderTest {

    private final long seed = 43L;
    private final int iterations = 1;
    private final long TIMEOUT_SECONDS = 4;

    @Ignore
    @Test
    public void setApiTest() {

        GraphManager gm = new GraphManager();

        // TODO: Figure out the real min and max for these
        // TODO: The HazelcastRequestsSchema needs to be regenerated.  It's currently using the minimal set of requests

        // Create Generator Stage (from Pronghorn Pipes).
        PipeConfig hzReqConfig = new PipeConfig(HazelcastRequestsSchema.instance, 5, 256);
        Pipe<HazelcastRequestsSchema> generatorPipe = new Pipe<>(hzReqConfig);
        PronghornStage generator = new EncoderTestGenerator(gm, seed, iterations, generatorPipe);

        // Create Splitter w/Pipes.
        Pipe<HazelcastRequestsSchema> pipeToExpecteds = new Pipe<>(hzReqConfig.grow2x());
        Pipe<HazelcastRequestsSchema> pipeToEncoder = new Pipe<>(hzReqConfig.grow2x());
        SplitterStage<HazelcastRequestsSchema> splitter = new SplitterStage(gm, generatorPipe, pipeToExpecteds, pipeToEncoder);

        // Create Encoder w/Pipes.
        PipeConfig rawBytes = new PipeConfig(RawDataSchema.instance, 5, 512);
        Pipe[] encoderToValidator = new Pipe[1];
        encoderToValidator[0] = new Pipe(rawBytes);
        // RequestEncodeStage is the class under test.
        new RequestEncodeStage(gm, pipeToEncoder, encoderToValidator, new Configurator());

        // The class  the expecteds w/Pipes
        Pipe expectedsToComparatorPipe = new Pipe(rawBytes);
        // TODO: Is the configurator really needed in Expecteds?
        new ExpectedsCreator(gm, pipeToExpecteds, expectedsToComparatorPipe, new Configurator());

        Pipe validatorToComparatorPipe = new Pipe(rawBytes);
        PronghornStage validate = new EncoderTestValidator<RawDataSchema>(gm, expectedsToComparatorPipe, encoderToValidator[0]);

        MonitorConsoleStage.attach(gm);

        System.out.println("running test");
        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        scheduler.startup();

        scheduler.awaitTermination(3, TimeUnit.SECONDS);
        System.out.println("finished running test");
    }
}
