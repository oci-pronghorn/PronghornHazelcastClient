package com.ociweb.hazelcast.stage;

import java.util.concurrent.TimeUnit;

import com.ociweb.hazelcast.HazelcastConfigurator;
import org.junit.*;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;

public class ConnectionTest {

    private static HazelcastInstance memberInstance;


    @BeforeClass
    public static void startupClusterMember() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");

        memberInstance = Hazelcast.newHazelcastInstance(config);

        if (0 == memberInstance.getAtomicLong("MyLock").getAndIncrement()) {
            System.out.println("Starting Connection Tets");
        }
    }

    @AfterClass
    public static void shutdownClusterMember() {
        System.out.println("shutting down cluster");
        memberInstance.shutdown();
    }


    @Test
    public void simpleAuthTest() {
        GraphManager gm = new GraphManager();

        PipeConfig<RawDataSchema> inputConfig = new PipeConfig(RawDataSchema.instance);
        PipeConfig<RequestResponseSchema> outputConfig = new PipeConfig(RequestResponseSchema.instance);

        Pipe<RawDataSchema> input = new Pipe<>(inputConfig);
        Pipe<RequestResponseSchema> output = new Pipe<>(outputConfig);

        HazelcastConfigurator conf = new HazelcastConfigurator();
        GeneratorStage gs = new GeneratorStage(gm, input, false);
        ConnectionStage cs = new TestConnectionStageWrapper(gm, input, output, conf, true); //shutdown is done here
        GraphManager.addNota(gm, GraphManager.PRODUCER, GraphManager.PRODUCER, cs);//need to kill this quickly

        PipeCleanerStage pc = new PipeCleanerStage(gm, output);

        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        scheduler.startup();

        scheduler.awaitTermination(3000, TimeUnit.SECONDS);

    }


    @Test
    public void simpleAuthAndPartitionRequestTest() {

        GraphManager gm = new GraphManager();

        PipeConfig<RawDataSchema> inputConfig = new PipeConfig(RawDataSchema.instance);
        PipeConfig<RequestResponseSchema> outputConfig = new PipeConfig(RequestResponseSchema.instance);

        Pipe<RawDataSchema> input = new Pipe<>(inputConfig);
        Pipe<RequestResponseSchema> output = new Pipe<>(outputConfig);

        HazelcastConfigurator conf = new HazelcastConfigurator();

        GeneratorStage gs = new GeneratorStage(gm, input, true); //shutdown is done here
        ConnectionStage cs = new TestConnectionStageWrapper(gm, input, output, conf, false);
        ConsoleJSONDumpStage<RequestResponseSchema> pc = new ConsoleJSONDumpStage<RequestResponseSchema>(gm, output);

        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        scheduler.startup();

        //this is time is a hack because I do not have a design yet to make this test exit exactly when the data shows up.
        try {
            Thread.sleep(3_000);
        } catch (InterruptedException e) {
        }

        scheduler.shutdown();

        scheduler.awaitTermination(60, TimeUnit.SECONDS);

    }


    public class TestConnectionStageWrapper extends ConnectionStage {

        private final boolean shutDownAfterAuth;
        protected TestConnectionStageWrapper(GraphManager graphManager,
                                             Pipe<RawDataSchema> input,
                                             Pipe<RequestResponseSchema> output,
                                             HazelcastConfigurator conf, boolean shutDownAfterAuth) {

            super(graphManager, input, output, conf);
            this.shutDownAfterAuth = shutDownAfterAuth;
        }

        @Override
        public void run() {
            super.run();

            if (shutDownAfterAuth && isAuthenticated) {
                Assert.assertTrue(authUUIDLen > 10);
                System.out.println("IP & GUID:" + authResponse);

                requestShutdown();

            }
        }

    }

    public class GeneratorStage extends PronghornStage {
        private boolean requestPartitions;
        private Pipe<RawDataSchema> output;

        protected GeneratorStage(GraphManager graphManager, Pipe<RawDataSchema> output, boolean requestPartitions) {
            super(graphManager, NONE, output);
            GraphManager.addNota(graphManager, GraphManager.PRODUCER, GraphManager.PRODUCER, this);
            this.output = output;
            this.supportsBatchedPublish = false;
            this.supportsBatchedRelease = false;
            this.requestPartitions = requestPartitions;
        }

        @Override
        public void run() {
            if (requestPartitions) {
                byte[] request = new byte[128]; //this is all zeros so zero length is allready set
                int messageType = 0x8; //request partitions
                // Curious -- I'm not sure why this sleep is necessary, but I(cas) don't have time to track it down
                // at the moment.  If I take this out, the cluster gets an NPE in OnData.  It may have something to
                // do with the platform I'm testing on -- it shows up in the Cloudbee's build as well, though --,
                // but a 3 second delay seems to provide enough time for the server -- or something -- to get set
                // up enough to handle the partition request. Curious, indeed.
                try {
                    Thread.sleep(3_000);
                } catch (InterruptedException ie) {
                }
                int len = ConnectionStage.writeHeader(request, 0, 42, -1, messageType);
                request[0] = (byte) len;

                Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
                Pipe.addByteArray(request, 0, len, output);
                System.err.println("ConnectionTest: send partition request");
                Pipe.publishWrites(output);
                requestPartitions = false;
            }
        }
    }
}
