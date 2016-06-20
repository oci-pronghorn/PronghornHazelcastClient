package com.ociweb.hazelcast;

import com.ociweb.hazelcast.stage.HazelcastRequestsSchema;
import com.ociweb.hazelcast.stage.RequestsProxy;

/**
 * This class provides access to the client-oriented, non-partition-oriented messages.
 */
public class ClientProxyHelper {
    /**
     * Create a client proxy for the various collections, locks, and events.
     * @param client is the instance holding the configurator for the requestor.
     * @param correlationId is the correlation ID to be used for this message request.
     * // N.B. This will probably change when the token hashing is implemented.
     * @param name is the UTF name to be sent to the server.
     * @param serviceName is the Hazelcast implementation name.
     * @return true if the request for the Client Proxy was successfully sent to the cluster, false otherwise.
     * Note that a successful send does _not_ mean the proxy was created.  The server will return a success response
     * when the proxy has been successfully created -- or an error if it was not.
     *
     */
    public static boolean createProxy(HazelcastClient client, int correlationId, CharSequence name, CharSequence serviceName) {
        RequestsProxy requestsProxy = client.getConfigurator().getRequestsProxy();
        if (requestsProxy.tryWriteFragment(HazelcastRequestsSchema.MSG_CREATEPROXY_5)) {
            return writeProxyInfo(client, correlationId, name, serviceName);
        } else {
            return false;
        }
    }

    /**
     * Destroy a proxy that was created earlier.
     * @param client is the instance holding the configurator for the requestor.
     * @param correlationId is the correlation ID for this request.
     * @param name is the name used during the creation of the proxy.
     * @param serviceName is the service name for the proxy.
     * @return true if the Destroy Proxy request was successfully sent to the cluster, false otherwise.
     * Note that a successful send does _not_ mean the proxy was destroyed.  The server will return a success response
     * when the proxy has been successfully destroyed -- or an error if it was not.
     */
    public static boolean destroyProxy(HazelcastClient client, int correlationId, CharSequence name, CharSequence serviceName) {
        RequestsProxy requestsProxy = client.getConfigurator().getRequestsProxy();
        if (requestsProxy.tryWriteFragment(HazelcastRequestsSchema.MSG_DESTROYPROXY_6)) {
            return writeProxyInfo(client, correlationId, name, serviceName);
        } else {
            return false;
        }
    }


    /**
     * Request a list of partitions from the cluster.
     * @param client
     * @param correlationId
     * @return
     */
    public static boolean getPartitions(HazelcastClient client, int correlationId) {
        RequestsProxy requestsProxy = client.getConfigurator().getRequestsProxy();
        if (requestsProxy.tryWriteFragment(HazelcastRequestsSchema.MSG_GETPARTITIONS_8)) {
            requestsProxy.writeInt(HazelcastRequestsSchema.MSG_GETPARTITIONS_8_FIELD_CORRELATIONID_2097136, correlationId);
            requestsProxy.writeInt(HazelcastRequestsSchema.MSG_GETPARTITIONS_8_FIELD_PARTITIONHASH_2097135, -1);
            requestsProxy.publishWrites();
            return true;
        } else {
            return false;
        }
    }

    private static boolean writeProxyInfo(HazelcastClient client, int correlationId, CharSequence name, CharSequence serviceName) {
        RequestsProxy requestsProxy = client.getConfigurator().getRequestsProxy();
        requestsProxy.writeInt(0x1, correlationId);
        requestsProxy.writeInt(0x2, -1);
        requestsProxy.writeUTF8(0x1400003, name);
        requestsProxy.writeUTF8(0x1400005, serviceName);
        requestsProxy.publishWrites();
        return true;
    }
}
