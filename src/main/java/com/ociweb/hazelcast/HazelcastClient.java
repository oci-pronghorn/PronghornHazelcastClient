package com.ociweb.hazelcast;

import com.ociweb.hazelcast.stage.HazelcastRequestsSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeWriter;

/**
 * Author: cas
 * Date:   12/17/15
 * Time:   19:16
 */
public class HazelcastClient {

    private final int maximumLengthOfVariableFields = 5;
    private int minimumNumberOfOutgoingFragments = 2;
    private static Pipe<HazelcastRequestsSchema> pipe;

    HazelcastClient() {
        PipeConfig<HazelcastRequestsSchema> hzReqConfig = new PipeConfig<>(HazelcastRequestsSchema.instance, minimumNumberOfOutgoingFragments, maximumLengthOfVariableFields);
        pipe = new Pipe<>(hzReqConfig);
    }


    public static boolean createProxy(int correlationId, CharSequence name, CharSequence serviceName) {
        if (PipeWriter.tryWriteFragment(pipe, 0x0)) {
            PipeWriter.writeInt(pipe, 0x1, correlationId);
            PipeWriter.writeInt(pipe, 0x2, -1);
            PipeWriter.writeUTF8(pipe, 0x1400003, name);
            PipeWriter.writeUTF8(pipe, 0x1400005, serviceName);
            return true;
        } else {
            return false;
        }
    }


    public static boolean destroyProxy(int correlationId, CharSequence name,
                                       CharSequence serviceName) {
        if (PipeWriter.tryWriteFragment(pipe, 0x6)) {
            PipeWriter.writeInt(pipe, 0x1, correlationId);
            PipeWriter.writeInt(pipe, 0x2, -1);
            PipeWriter.writeUTF8(pipe, 0x1400003, name);
            PipeWriter.writeUTF8(pipe, 0x1400005, serviceName);
            return true;
        } else {
            return false;
        }
    }


    public static boolean getPartitions(int correlationId) {
        if (PipeWriter.tryWriteFragment(pipe, 0xc)) {
            PipeWriter.writeInt(pipe, 0x1, correlationId);
            PipeWriter.writeInt(pipe, 0x2, -1);
            return true;
        } else {
            return false;
        }
    }
}

