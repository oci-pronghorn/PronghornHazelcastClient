package com.ociweb.hazelcast.stage;

import java.net.InetSocketAddress;

import com.ociweb.pronghorn.pipe.RawDataSchema;
import org.junit.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.ociweb.hazelcast.stage.util.InetSocketAddressImmutable;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ConnectionTest {

    @Test
    public void simpleAuthTest() {

       // LoginModuleConfig loginModuleConfig = new LoginModuleConfig();

       // SecurityConfig securityConfig = new SecurityConfig();
       // securityConfig.addClientLoginModuleConfig(loginModuleConfig);
        //TODO: need more details on how to setup cluster needing password.

        Config config = new Config();

        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");

        final int port = config.getNetworkConfig().getPort();

        System.err.println("port:"+port);

       // config.setSecurityConfig(securityConfig);
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        if (0==hazelcastInstance.getAtomicLong("MyLock").getAndIncrement()) {
            System.out.println("We are started!");
        }

        ///
        //build up instance of ConnectionStage to be tested
        //

        GraphManager gm = new GraphManager();

        PipeConfig<RawDataSchema> inputConfig = new PipeConfig(RawDataSchema.instance);
        PipeConfig<RequestResponseSchema> outputConfig = new PipeConfig(RequestResponseSchema.instance);;

        Pipe<RawDataSchema> input = new Pipe<>(inputConfig);
        input.initBuffers(); //Must be done manually because we are not using a scheduler for this test

        Pipe<RequestResponseSchema> output = new Pipe<>(outputConfig);
        output.initBuffers(); //Must be done manually because we are not using a scheduler for this test

/*
        HazelcastConfigurator conf = new HazelcastConfigurator() {

            public InetSocketAddress buildInetSocketAddress(int stageId) {
                return new InetSocketAddressImmutable("127.0.0.1",port);
             }

            public CharSequence getUUID(int stageId) {
                return "ThisIsMe";
            }

            public CharSequence getOwnerUUID(int stageId) {
                return "ThisIsNotMe";
            }

            public CharSequence getUserName(int stageId) {
                return "dev"; //default value
            }

            public CharSequence getPassword(int stageId) {
                return "dev-pass"; //default value
            }


            public boolean isCustomAuth() {
                return false;

            }
        };
*/
        HazelcastConfigurator conf = new HazelcastConfigurator();
        ConnectionStage cs = new ConnectionStage(gm, input, output, conf);

        //started up with login credentials
        cs.startup(); //you can change passwords but this will require a new graph instance.

        int seconds = 10;
        long testEnd = System.currentTimeMillis() + (1000 * seconds);
        while (System.currentTimeMillis()<testEnd) {
            cs.run();
        }

        System.err.println("now exiting");

    }

}
