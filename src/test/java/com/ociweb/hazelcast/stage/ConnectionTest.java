package com.ociweb.hazelcast.stage;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;

public class ConnectionTest {

    private static HazelcastInstance memberInstance;
    
    
    @BeforeClass
    public static void statupClusterMember() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        
        memberInstance = Hazelcast.newHazelcastInstance(config);
        
        if (0==memberInstance.getAtomicLong("MyLock").getAndIncrement()) {
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
        PipeConfig<RequestResponseSchema> outputConfig = new PipeConfig(RequestResponseSchema.instance);;

        Pipe<RawDataSchema> input = new Pipe<>(inputConfig);
        Pipe<RequestResponseSchema> output = new Pipe<>(outputConfig);

        HazelcastConfigurator conf = new HazelcastConfigurator();
        ConnectionStage cs = new TestConnectionStageWrapper(gm, input, output, conf, true);
        GeneratorStage gs = new GeneratorStage(gm, input);
        PipeCleanerStage pc = new PipeCleanerStage(gm, output);
        
        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        scheduler.startup();
      
        scheduler.awaitTermination(3,TimeUnit.SECONDS);
 
    }
    
    
    public class TestConnectionStageWrapper extends ConnectionStage {
        
        private final boolean shutDownAfterAuth;
        
        protected TestConnectionStageWrapper(GraphManager graphManager,
                Pipe<RawDataSchema> input,
                Pipe<RequestResponseSchema> output,
                HazelcastConfigurator conf, boolean shutDownAfterAuth) {
            super(graphManager,input,output,conf);
            this.shutDownAfterAuth = shutDownAfterAuth;
        }
        
        @Override
        public void run() {
            super.run();
            
            if (shutDownAfterAuth && isAuthenticated) {
                Assert.assertTrue(authUUIDLen>10);
                System.out.println("IP & GUID:"+authResponse);
                
                requestShutdown();
                
            }            
        }

    }
    
    public class GeneratorStage extends PronghornStage {

        protected GeneratorStage(GraphManager graphManager, Pipe output) {
            super(graphManager, NONE, output);
            GraphManager.addNota(graphManager,GraphManager.PRODUCER,GraphManager.PRODUCER, this);
        }

        @Override
        public void run() {
            
        }
        
    }
    

}
