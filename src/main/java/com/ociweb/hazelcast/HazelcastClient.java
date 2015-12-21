package com.ociweb.hazelcast;

import com.ociweb.hazelcast.stage.HazelcastRequestsSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class HazelcastClient {

    private int maximumLengthOfHzVariableFields = 1024;
    private int minimumNumberOfHzOutgoingFragments = 2;
    private static Pipe<HazelcastRequestsSchema> requestPipe;
    private static Pipe<HazelcastRequestsSchema> responsePipe;

    private int maximumLengthOfRawVariableFields = 2048;
    private int minimumNumberOfRawOutgoingFragments = 1;
    private static Pipe<RawDataSchema> encoderToConnectionPipe;
    private static Pipe<RawDataSchema> connectionToDecoderPipe;

    GraphManager gm = new GraphManager();

    HazelcastClient(HazelcastClientConfig config) {
        PipeConfig<HazelcastRequestsSchema> hzProtocolConfig =
            new PipeConfig<>(HazelcastRequestsSchema.instance, minimumNumberOfHzOutgoingFragments, maximumLengthOfHzVariableFields);
        requestPipe = new Pipe<>(hzProtocolConfig);
        responsePipe = new Pipe<>(hzProtocolConfig);

        PipeConfig<RawDataSchema> rawDataConfig =
            new PipeConfig<>(RawDataSchema.instance, minimumNumberOfRawOutgoingFragments, maximumLengthOfRawVariableFields);


    }

    public CharSequence getName(int token) {
        //TODO: This returns the UTF8 name associated with this token.
        return null;
    }


    private static class Client {
        public static boolean createProxy(int correlationId, CharSequence name, CharSequence serviceName) {
            if (PipeWriter.tryWriteFragment(requestPipe, 0x0)) {
                return writeProxyInfo(correlationId, name, serviceName);
            } else {
                return false;
        }
    }

    public static boolean destroyProxy(int correlationId, CharSequence name, CharSequence serviceName) {
        if (PipeWriter.tryWriteFragment(requestPipe, 0x6)) {
            return writeProxyInfo(correlationId, name, serviceName);
        } else {
            return false;
        }
    }


    public static boolean getPartitions(int correlationId) {
        if (PipeWriter.tryWriteFragment(requestPipe, 0xc)) {
            PipeWriter.writeInt(requestPipe, 0x1, correlationId);
            PipeWriter.writeInt(requestPipe, 0x2, -1);
            return true;
        } else {
            return false;
        }
    }


    private static boolean writeProxyInfo(int correlationId, CharSequence name, CharSequence serviceName) {
        PipeWriter.writeInt(requestPipe, 0x1, correlationId);
        PipeWriter.writeInt(requestPipe, 0x2, -1);
        PipeWriter.writeUTF8(requestPipe, 0x1400003, name);
        PipeWriter.writeUTF8(requestPipe, 0x1400005, serviceName);
        return true;
    }
    }
}

