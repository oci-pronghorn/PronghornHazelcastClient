package com.ociweb.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.ociweb.hazelcast.stage.ResponseCallBack;
import com.ociweb.hazelcast.stage.util.MidAmble;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HazelcastSetTestX {

    private static HazelcastInstance hazelcastInstance;
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

    @AfterClass
    public static void stopServer() {
        hazelcastInstance.shutdown();
    }

    @Test
    public void createSet() {
        // ToDo: The placeholder name will change after Configuration from a file
        // is implemented.
        client = new HazelcastClient("PathToConfigurationFile", new SetTestCallBack());
        int setSize = -1;
        int cid = 1;

        // ToDo: Figure out a better way to give the server time to spin up before running.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }


        final int fstoken = HazelcastSet.createNewSet(client, cid, "FirstSet");
        if (-1 == fstoken) {
            fail("Unable to get token, see log");
        }

        int EnoughForNow = 3;
        int numberOfTimes = 0;
        while (!goOn) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
            }
            numberOfTimes++;
            if (numberOfTimes == EnoughForNow) break;
        }
        goOn = false;

        // ToDo: Waiting on implementation of partition hashing
        // Add a string -- must be serializable or identifiable as serializable or portable.
//        assertTrue(HazelcastSet.add(client, cid, fstoken, "MyStringValue"));

        // ToDo: Waiting on implementation of partition hashing
        // Request the size. The callback will get the response.
//        assertTrue(HazelcastSet.size(client, cid, fstoken));

        try {
            Thread.sleep(2000L);
        } catch (InterruptedException ie) {
            // no big deal, just move on.
        }
    }

    private class SetTestCallBack implements ResponseCallBack {
        @Override
        public void send(int correlationId, short type, short flags, int partitionId, HZDataInput dataSource) {
            // assert((short)0x000C = flags) : "flags are not start and end, actual values: " + flags);
            System.err.println("SetTestX: successful callback received ");
            System.err.println("cb correlationId: " + correlationId);
            System.err.printf("cb type: 0x%X\n", type);
            System.err.printf("cb flags: 0x%X\n", flags);
            System.err.printf("cb partitionId: %d(0x%X)\n", partitionId, partitionId);
            System.err.println("cb dataSource Length: " + dataSource.length());

/*
            if (dataSource.length() > 0) {
                try {
                    int partition = dataSource.readInt();
                    String partitionName = dataSource.readUTF();
                } catch (IOException e) {
                    System.err.println("SetTestCallBack: IOException");
                    e.printStackTrace();
                }
            }
*/
            goOn = true;
        }
    }
}
