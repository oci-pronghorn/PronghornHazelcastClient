package com.ociweb.hazelcast;

import com.ociweb.hazelcast.stage.RequestsProxy;
import com.ociweb.hazelcast.stage.ResponseCallBack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HazelcastClient provides a holder for the configuration for this
 * set of pipes to a Hazelcast cluster and provides access to a proxy to
 * send requests to the server.
 */
public class HazelcastClient {
    private final static Logger log = LoggerFactory.getLogger(HazelcastClient.class);

    private HazelcastConfigurator config;

    /**
     * Convenience creation for a non-customized configuration.
     * @param callBack provides the location to send return information from the cluster.
     */
    public HazelcastClient(ResponseCallBack callBack) {
        this(null, callBack);
    }

    public HazelcastClient(String configurationFileName, ResponseCallBack callBack) {
        config = new HazelcastConfigurator(configurationFileName, callBack);
    }

    public HazelcastConfigurator getConfigurator() {
        return config;
    }
}
