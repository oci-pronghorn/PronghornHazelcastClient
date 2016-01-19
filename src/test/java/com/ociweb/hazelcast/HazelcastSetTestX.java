package com.ociweb.hazelcast;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.ociweb.hazelcast.stage.HazelcastClient;
import com.ociweb.hazelcast.stage.ResponseCallBack;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HazelcastSetTestX {

    private static HazelcastInstance hazelcastInstance;
    private HazelcastClientConfig config;
    private HazelcastClient client;
    private boolean goOn = false;

    @BeforeClass
    public static void startServer() {
        Config config = new Config();
        config.setGroupConfig(new GroupConfig("dev","dev-pass"));
        NetworkConfig nwConfig = config.getNetworkConfig();
        nwConfig.setPort(5701);
        config.setNetworkConfig(nwConfig);
        final int port = config.getNetworkConfig().getPort();
        System.err.println("port:" + port);
       // config.setSecurityConfig(securityConfig);
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void createSet() {
        config = new HazelcastClientConfig("path to config");
        client = new HazelcastClient(config, new SetTestCallBack());
        int setSize = -1;
        int cid = 1;

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }


        final int fstoken = client.newSet(client, cid, "FirstSet");
        if (-1 == fstoken) {
            fail("Unable to get token, see log");
        }

        int ThatsEnoughForNow = 3;
        int numberOfTimes = 0;
        while (!goOn) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
            }
            numberOfTimes++;
            if (numberOfTimes == ThatsEnoughForNow) break;
        }
        goOn = false;

        // Add a string, Must be serializable or identifiable serializable or portable..
//        assertTrue(HazelcastSet.add(client, cid, fstoken, "MyStringValue"));

        // Add a low level object, can be used for very tight serialization
/*
        DataOutputStream out = HazelcastSet.add(client, cid, fstoken);
        out.writeLong(42);
        out.close();
*/

        //request the size and the callback will get the response
//        assertTrue(HazelcastSet.size(client, cid, fstoken));

        try {
            Thread.sleep(20000L);
        } catch (InterruptedException ie) {
            // no big deal
        }
    }

    private class SetTestCallBack implements ResponseCallBack {
        @Override
        public void send(int correlationId, short type, short flags, int partitionId, HZDataInput dataSource) {
            // assert((short)0x000C = flags) : "flags are not start and end, actual values: " + flags);
            System.err.println("SetTestX: callback");
            System.err.println("correlationId: " + correlationId);
            System.err.printf("type: 0x%X\n", type);
            System.err.printf("flags: 0x%X\n", flags);
            System.err.printf("paritionId: %d(0x%X)\n", partitionId, partitionId);
            System.err.println("dataSource Length: " + dataSource.length());
            if (dataSource.length() > 0) {
                try {
                    int partition = dataSource.readInt();
                    String partitionName = dataSource.readUTF();
                } catch (IOException e) {
                    System.err.println("SetTestCallBack: IOException");
                    e.printStackTrace();
                }
            }

            goOn = true;
        }
    }

    @AfterClass
    public static void stopServer() {
        hazelcastInstance.shutdown();
    }

}
