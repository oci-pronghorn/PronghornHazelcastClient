package com.ociweb.hazelcast;

import com.ociweb.hazelcast.stage.HazelcastRequestsSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeWriter;

import java.nio.ByteBuffer;

public class HazelcastSet {

    private static final int minimumNumberOfOutgoingFragments = 2;
    private static final int maximumLengthOfVariableFields = 1024;
    private static Pipe<HazelcastRequestsSchema> pipe;

    HazelcastSet() {
        PipeConfig<HazelcastRequestsSchema> hzReqConfig = new PipeConfig<>(HazelcastRequestsSchema.instance, minimumNumberOfOutgoingFragments, maximumLengthOfVariableFields);
        pipe = new Pipe<>(hzReqConfig);
    }


    public static boolean size(int correlationId, CharSequence name) {
        if (PipeWriter.tryWriteFragment(pipe, 0x10)) {
            PipeWriter.writeInt(pipe, 0x1, correlationId);
            PipeWriter.writeInt(pipe, 0x2, -1);
            PipeWriter.writeUTF8(pipe, 0x1400003, name);
            return true;
        } else {
            return false;
        }
    }


    public static boolean contains(int correlationId, CharSequence name, ByteBuffer value) {
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


    public static boolean containsAll(int correlationId, CharSequence name, ByteBuffer valueSet) {
        if (PipeWriter.tryWriteFragment(pipe, 0x1b)) {
            PipeWriter.writeInt(pipe, 0x1, correlationId);
            PipeWriter.writeInt(pipe, 0x2, -1);
            PipeWriter.writeUTF8(pipe, 0x1400003, name);
            PipeWriter.writeBytes(pipe, 0x1c00005, valueSet);
            return true;
        } else {
            return false;
        }
    }


    public static boolean add(int correlationId, CharSequence name, ByteBuffer value) {
        if (PipeWriter.tryWriteFragment(pipe, 0x21)) {
            PipeWriter.writeInt(pipe, 0x1, correlationId);
            PipeWriter.writeInt(pipe, 0x2, -1);
            PipeWriter.writeUTF8(pipe, 0x1400003, name);
            PipeWriter.writeBytes(pipe, 0x1c00005, value);
            return true;
        } else {
            return false;
        }
    }
}

