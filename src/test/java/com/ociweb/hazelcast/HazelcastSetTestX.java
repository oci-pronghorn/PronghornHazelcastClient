package com.ociweb.hazelcast;

import com.ociweb.hazelcast.stage.HazelcastClient;
import com.ociweb.hazelcast.stage.ResponseCallBack;
import org.junit.Test;

import java.io.IOException;

public class HazelcastSetTestX {

    private HazelcastClientConfig config;
    private HazelcastClient client;

    @Test
    public void createSet() {
        config = new HazelcastClientConfig("path to config");
        client = new HazelcastClient(config, new SetTestCallBack());
        int setSize = -1;
        int cid = 1;

        int fstoken = client.newSet(cid, "FirstSet");
        // Add a string, Must be serializable or identifiable serializble or portable..
        HazelcastSet.add(client, cid, fstoken, "MyStringValue");

        // Add a low level object, can be used for very tight serialization
/*
        DataOutputStream out = HazelcastSet.add(client, cid, fstoken);
        out.writeLong(42);
        out.close();
*/

        //request the size and the callback will get the response
        HazelcastSet.size(client, cid, fstoken);
    }

    private class SetTestCallBack implements ResponseCallBack {

        @Override
        public void send(int correlationId, short type, short flags, int partitionId, HZDataInput dataSource) {
            // assert((short)0x000C = flags) : "flags are not start and end, actual values: " + flags);
        }
    }

}
