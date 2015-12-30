package com.ociweb.hazelcast;

import com.ociweb.hazelcast.stage.HazelcastClient;
import com.ociweb.hazelcast.stage.HazelcastRequestsSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class HazelcastSet {

    public static boolean size(HazelcastClient client, int correlationId, int token) {
        Pipe<HazelcastRequestsSchema> pipe = client.getRequestPipe();
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
        Pipe<HazelcastRequestsSchema> pipe = client.getRequestPipe();
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
        Pipe<HazelcastRequestsSchema> pipe = client.getRequestPipe();
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
        byte[] bytes = {'A', 'B', 'C'};

        Pipe<HazelcastRequestsSchema> pipe = client.getRequestPipe();
        if (PipeWriter.tryWriteFragment(pipe, 0x21)) {
            PipeWriter.writeInt(pipe, 0x1, correlationId);
            PipeWriter.writeInt(pipe, 0x2, -1);
            PipeWriter.writeUTF8(pipe, 0x1400003, client.getName(token));
            // TODO:  MOcked -- Serialize value for real soon.
            PipeWriter.writeBytes(pipe, 0x1c00005, bytes, 0, bytes.length, Pipe.blobMask(pipe));
            PipeWriter.publishWrites(pipe);
            return true;
        } else {
            return false;
        }
    }

}

