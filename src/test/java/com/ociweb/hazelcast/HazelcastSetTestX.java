package com.ociweb.hazelcast;

import com.ociweb.hazelcast.stage.HazelcastClient;

public class HazelcastSetTestX {

    private static HazelcastClientConfig config;
    private static HazelcastClient client;

    public static void main(String[] args) {
        config = new HazelcastClientConfig("path to config");
        client = new HazelcastClient(config);
        int setSize = -1;

        HazelcastResponseHandler responseHandler = new HazelcastResponseHandler();

        int cid = 1;

//        HazelcastClient.registerCallBack(config, cid, responseHandler);
//        //For monitoring or getting all responses from cluster
//        HazelcastClient.registerCallBackMonitorAll(config, responseForAll);
//        //For only getting those that do not have a CallBack for the correlation id registered
//        HazelcastClient.registerCallBackDefault(config, responseForWhenCorrelationIdNotFound);
//
//        //at some point to remove listener can call
//        HazelcastClient.clearCallBack(config, cid);


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
}
