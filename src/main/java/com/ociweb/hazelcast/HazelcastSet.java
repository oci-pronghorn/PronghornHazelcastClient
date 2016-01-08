package com.ociweb.hazelcast;

import com.ociweb.hazelcast.stage.HazelcastClient;
import com.ociweb.hazelcast.stage.HazelcastRequestsSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;

import java.io.*;
import java.nio.ByteBuffer;

public class HazelcastSet {

    public static boolean size(HazelcastClient client, int correlationId, int token) {
        if (client.getRequestsProxy().tryWriteFragment(0x10)) {
            client.getRequestsProxy().writeInt(0x1, correlationId);
            client.getRequestsProxy().writeInt(0x2, -1);
            client.getRequestsProxy().writeInt(0x3, token);
            client.getRequestsProxy().writeUTF8(0x1400004, client.getName(token));
            client.getRequestsProxy().publishWrites();
            return true;
        } else {
            return false;
        }
    }


    public static boolean contains(HazelcastClient client, int correlationId, int token, CharSequence name, ByteBuffer value) {
        if (client.getRequestsProxy().tryWriteFragment(0x15)) {
            client.getRequestsProxy().writeInt(0x1, correlationId);
            client.getRequestsProxy().writeInt(0x2, -1);
            client.getRequestsProxy().writeInt(0x3, token);
            client.getRequestsProxy().writeUTF8(0x1400004, client.getName(token));
            client.getRequestsProxy().writeBytes(0x1c00006, value);
            return true;
        } else {
            return false;
        }
    }


    public static boolean containsAll(HazelcastClient client, int correlationId, int token, ByteBuffer valueSet) {
        if (client.getRequestsProxy().tryWriteFragment(0x1b)) {
            client.getRequestsProxy().writeInt(0x1, correlationId);
            client.getRequestsProxy().writeInt(0x2, -1);
            client.getRequestsProxy().writeInt(0x3, token);
            client.getRequestsProxy().writeUTF8(0x1400004, client.getName(token));
            client.getRequestsProxy().writeBytes(0x1c00006, valueSet);
            return true;
        } else {
            return false;
        }
    }


    public static boolean add(HazelcastClient client, int correlationId, int token, Serializable value)  {
        if (client.getRequestsProxy().tryWriteFragment(0x21)) {
            client.getRequestsProxy().writeInt(0x1, correlationId);
            client.getRequestsProxy().writeInt(0x2, -1);
            // FUTURE: The following two lines should be used when tokens go into play
//            PipeWriter.writeInt(pipe, 0x3, token);
//            PipeWriter.writeUTF8(pipe, 0x1400004, client.getName(token));
            client.getRequestsProxy().writeUTF8(0x1400004, client.getName(token));
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                oos.writeObject(value);
                byte[] valueBytes = bos.toByteArray();
                client.getRequestsProxy().writeBytes(0x1c00006, valueBytes, 0, valueBytes.length);
                client.getRequestsProxy().publishWrites();
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

    // TODO:  Implement
    public static boolean add(HazelcastClient client, int correlationId, int token, Externalizable value)  {
        return false;
    }
}

