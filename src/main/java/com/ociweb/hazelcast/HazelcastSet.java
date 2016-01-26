package com.ociweb.hazelcast;

import com.ociweb.hazelcast.stage.HazelcastRequestsSchema;
import com.ociweb.hazelcast.stage.RequestsProxy;

import java.io.*;
import java.nio.ByteBuffer;

public class HazelcastSet {

    public static boolean size(HazelcastClient client, int correlationId, int token) {
        RequestsProxy proxy = client.getConfigurator().getRequestsProxy();
        if (proxy.tryWriteFragment(HazelcastRequestsSchema.MSG_SIZE_1537)) {
            proxy.writeInt(HazelcastRequestsSchema.MSG_SIZE_1537_FIELD_CORRELATIONID_2097136, correlationId);
            proxy.writeInt(HazelcastRequestsSchema.MSG_SIZE_1537_FIELD_PARTITIONHASH_2097135, -1);
            proxy.writeUTF8(HazelcastRequestsSchema.MSG_SIZE_1537_FIELD_NAME_458497, client.getConfigurator().getName(token));
            proxy.publishWrites();
            return true;
        } else {
            return false;
        }
    }


    public static boolean contains(HazelcastClient client, int correlationId, int token, CharSequence name, ByteBuffer value) {
        RequestsProxy proxy = client.getConfigurator().getRequestsProxy();
        if (proxy.tryWriteFragment(HazelcastRequestsSchema.MSG_CONTAINS_1538)) {
            proxy.writeInt(HazelcastRequestsSchema.MSG_CONTAINS_1538_FIELD_CORRELATIONID_2097136, correlationId);
            proxy.writeInt(HazelcastRequestsSchema.MSG_CONTAINS_1538_FIELD_PARTITIONHASH_2097135, -1);
            proxy.writeUTF8(HazelcastRequestsSchema.MSG_CONTAINS_1538_FIELD_NAME_458497, client.getConfigurator().getName(token));
            proxy.writeBytes(HazelcastRequestsSchema.MSG_CONTAINS_1538_FIELD_VALUE_458498, value);
            return true;
        } else {
            return false;
        }
    }


    public static boolean containsAll(HazelcastClient client, int correlationId, int token, ByteBuffer valueSet) {
        RequestsProxy proxy = client.getConfigurator().getRequestsProxy();
        if (proxy.tryWriteFragment(HazelcastRequestsSchema.MSG_CONTAINSALL_1539)) {
            proxy.writeInt(HazelcastRequestsSchema.MSG_CONTAINSALL_1539_FIELD_CORRELATIONID_2097136, correlationId);
            proxy.writeInt(HazelcastRequestsSchema.MSG_CONTAINSALL_1539_FIELD_PARTITIONHASH_2097135, -1);
            proxy.writeUTF8(HazelcastRequestsSchema.MSG_CONTAINSALL_1539_FIELD_NAME_458497, client.getConfigurator().getName(token));
            proxy.writeBytes(HazelcastRequestsSchema.MSG_CONTAINSALL_1539_FIELD_VALUESET_458499, valueSet);
            return true;
        } else {
            return false;
        }
    }


    public static boolean add(HazelcastClient client, int correlationId, int token, Serializable value)  {
        RequestsProxy proxy = client.getConfigurator().getRequestsProxy();
        if (proxy.tryWriteFragment(HazelcastRequestsSchema.MSG_ADD_1540)) {
            proxy.writeInt(HazelcastRequestsSchema.MSG_ADD_1540_FIELD_CORRELATIONID_2097136, correlationId);
            proxy.writeInt(HazelcastRequestsSchema.MSG_ADD_1540_FIELD_PARTITIONHASH_2097135, -1);
            proxy.writeUTF8(HazelcastRequestsSchema.MSG_ADD_1540_FIELD_NAME_458497,client.getConfigurator().getName(token));
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                oos.writeObject(value);
                byte[] valueBytes = bos.toByteArray();
                proxy.writeBytes(HazelcastRequestsSchema.MSG_ADD_1540_FIELD_VALUE_458498, valueBytes, 0, valueBytes.length);
                proxy.publishWrites();
                return true;
            } catch (IOException e) {
                // ToDo: handle this better for some value of better
                e.printStackTrace();
                return false;
            }
        } else {
            return false;
        }
    }

    // TODO:  Implement an add for each of the serializable types
    public static boolean add(HazelcastClient client, int correlationId, int token, Externalizable value)  {
        return false;
    }
}

