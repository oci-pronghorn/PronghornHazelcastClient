package com.ociweb.hazelcast;

import com.ociweb.hazelcast.stage.ResponseCallBack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HazelcastClient holds and provides access to the configuration for
 * this set of pipes to a Hazelcast cluster.
 */
public class HazelcastClient {
    private final static Logger log = LoggerFactory.getLogger(HazelcastClient.class);

    private HazelcastConfigurator config;

    /**
     * Convenience construction of a non-customized configuration.
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
