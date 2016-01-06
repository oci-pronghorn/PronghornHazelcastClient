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
        Pipe<HazelcastRequestsSchema> pipe = client.getRequestPipe();
        if (PipeWriter.tryWriteFragment(pipe, 0x10)) {
            PipeWriter.writeInt(pipe, 0x1, correlationId);
            PipeWriter.writeInt(pipe, 0x2, -1);
            PipeWriter.writeInt(pipe, 0x3, token);
            PipeWriter.writeUTF8(pipe, 0x1400004, client.getName(token));
            return true;
        } else {
            return false;
        }
    }


    public static boolean contains(HazelcastClient client, int correlationId, int token, CharSequence name, ByteBuffer value) {
        Pipe<HazelcastRequestsSchema> pipe = client.getRequestPipe();
        if (PipeWriter.tryWriteFragment(pipe, 0x15)) {
            PipeWriter.writeInt(pipe, 0x1, correlationId);
            PipeWriter.writeInt(pipe, 0x2, -1);
            PipeWriter.writeInt(pipe, 0x3, token);
            PipeWriter.writeUTF8(pipe, 0x1400004, client.getName(token));
            PipeWriter.writeBytes(pipe, 0x1c00006, value);
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
            PipeWriter.writeInt(pipe, 0x3, token);
            PipeWriter.writeUTF8(pipe, 0x1400004, client.getName(token));
            PipeWriter.writeBytes(pipe, 0x1c00006, valueSet);
            return true;
        } else {
            return false;
        }
    }


    public static boolean add(HazelcastClient client, int correlationId, int token, Serializable value)  {
        Pipe<HazelcastRequestsSchema> pipe = client.getRequestPipe();
        if (PipeWriter.tryWriteFragment(pipe, 0x21)) {
            PipeWriter.writeInt(pipe, 0x1, correlationId);
            PipeWriter.writeInt(pipe, 0x2, -1);
            // FUTURE: The following two lines should be used when tokens go into play
//            PipeWriter.writeInt(pipe, 0x3, token);
//            PipeWriter.writeUTF8(pipe, 0x1400004, client.getName(token));
            PipeWriter.writeUTF8(pipe, 0x1400004, client.getName(token));
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                oos.writeObject(value);
                byte[] valueBytes = bos.toByteArray();
                PipeWriter.writeBytes(pipe, 0x1c00006, valueBytes, 0, valueBytes.length, Pipe.blobMask(pipe));
                PipeWriter.publishWrites(pipe);
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
        Pipe<HazelcastRequestsSchema> pipe = client.getRequestPipe();
        return false;
    }
}

