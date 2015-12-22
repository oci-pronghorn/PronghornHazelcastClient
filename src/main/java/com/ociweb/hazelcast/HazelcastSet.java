package com.ociweb.hazelcast;

import com.ociweb.hazelcast.stage.HazelcastClient;
import com.ociweb.hazelcast.stage.HazelcastRequestsSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class HazelcastSet {

    private static final int minimumNumberOfOutgoingFragments = 2;
    private static final int maximumLengthOfVariableFields = 1024;
    private static Pipe<HazelcastRequestsSchema> pipe;


    public static int newSet(HazelcastClient client, int correlationId, CharSequence name) {
        // use the client to request a Set from the proxy, the client returns a token which will be returned.
        return 0;
    }

    public static boolean size(HazelcastClient client, int correlationId, int token) {
        if (PipeWriter.tryWriteFragment(pipe, 0x10)) {
            PipeWriter.writeInt(pipe, 0x1, correlationId);
            PipeWriter.writeInt(pipe, 0x2, -1);
            PipeWriter.writeUTF8(pipe, 0x1400003, client.getName(token));
            return true;
        } else {
            return false;
        }
    }


    public static boolean contains(HazelcastClient client, int correlationId, CharSequence name, ByteBuffer value) {
        if (PipeWriter.tryWriteFragment(pipe, 0x15)) {
            PipeWriter.writeInt(pipe, 0x1, correlationId);
            PipeWriter.writeInt(pipe, 0x2, -1);
            PipeWriter.writeUTF8(pipe, 0x1400003, name);
            PipeWriter.writeBytes(pipe, 0x1c00005, value);
            return true;
        } else {
            return false;
        }
    }


    public static boolean containsAll(HazelcastClient client, int correlationId, int token, ByteBuffer valueSet) {
        if (PipeWriter.tryWriteFragment(pipe, 0x1b)) {
            PipeWriter.writeInt(pipe, 0x1, correlationId);
            PipeWriter.writeInt(pipe, 0x2, -1);
            PipeWriter.writeUTF8(pipe, 0x1400003, client.getName(token));
            PipeWriter.writeBytes(pipe, 0x1c00005, valueSet);
            return true;
        } else {
            return false;
        }
    }


    public static boolean add(HazelcastClient client, int correlationId, int token, Serializable value)  {
        if (PipeWriter.tryWriteFragment(pipe, 0x21)) {
            PipeWriter.writeInt(pipe, 0x1, correlationId);
            PipeWriter.writeInt(pipe, 0x2, -1);
            PipeWriter.writeUTF8(pipe, 0x1400003, client.getName(token));
            // Serialize this
//            PipeWriter.writeBytes(pipe, 0x1c00005, )
            return true;
        } else {
            return false;
        }
    }

}

