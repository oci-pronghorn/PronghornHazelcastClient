package com.ociweb.hazelcast.stage;

import com.ociweb.hazelcast.HazelcastClientConfig;
import com.ociweb.hazelcast.stage.HazelcastConfigurator;
import com.ociweb.hazelcast.stage.ConnectionStage;
import com.ociweb.hazelcast.stage.HazelcastRequestsSchema;
import com.ociweb.hazelcast.stage.RequestEncodeStage;
import com.ociweb.hazelcast.stage.util.LittleEndianByteHelpers;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HazelcastClient {

    private static int maximumLengthOfHzVariableFields = 1024;
    private static int minimumNumberOfHzOutgoingFragments = 2;
    private static Pipe<HazelcastRequestsSchema> requestPipe;
    private static Pipe<HazelcastRequestsSchema> responsePipe;

    private static int maximumLengthOfRawVariableFields = 2048;
    private static int minimumNumberOfRawOutgoingFragments = 5;
    private static Pipe<RawDataSchema> encoderToConnectionPipe;
    private static Pipe<RawDataSchema> connectionToDecoderPipe;

    private GraphManager gm = new GraphManager();
    private HazelcastConfigurator configurator;

    private AtomicInteger token = new AtomicInteger(0);
    private byte[] midAmbles;
    private int penultimateMidAmbleEntry = 0;

    public HazelcastClient(HazelcastClientConfig config) {

        configurator = new HazelcastConfigurator();
        midAmbles = new byte[(configurator.getMaxMidAmble() * 50)];
        penultimateMidAmbleEntry = midAmbles.length - (configurator.getMaxMidAmble());

        HazelcastClient.buildClientGraph(gm, configurator);
        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        scheduler.startup();

        // ToDo: TEMP -- remove this later, but until this is running ...
        scheduler.awaitTermination(300, TimeUnit.SECONDS);
    }

    public static void buildClientGraph(GraphManager gm, HazelcastConfigurator configurator) {
        PipeConfig<HazelcastRequestsSchema> hzProtocolConfig =
            new PipeConfig<>(HazelcastRequestsSchema.instance, minimumNumberOfHzOutgoingFragments, maximumLengthOfHzVariableFields);
        requestPipe = new Pipe<>(hzProtocolConfig);
        responsePipe = new Pipe<>(hzProtocolConfig);

        PipeConfig<RawDataSchema> rawDataConfig =
            new PipeConfig<>(RawDataSchema.instance, minimumNumberOfRawOutgoingFragments, maximumLengthOfRawVariableFields);

        for (int pipeNumber = 1; pipeNumber <= configurator.getNumberOfConnectionStages(); pipeNumber++) {
            configurator.encoderToConnectionPipes[pipeNumber] = new Pipe<>(rawDataConfig);
            configurator.connectionToDecoderPipes[pipeNumber] = new Pipe<>(rawDataConfig);
            configurator.connectionStage[pipeNumber] =
                new ConnectionStage(gm, configurator.encoderToConnectionPipes[pipeNumber], configurator.connectionToDecoderPipes[pipeNumber], configurator);
        }

        new RequestEncodeStage(gm, requestPipe,  configurator.encoderToConnectionPipes, configurator);
        new RequestDecodeStage(gm, configurator.connectionToDecoderPipes, configurator);

        MonitorConsoleStage.attach(gm);
    }


    public CharSequence getName(int token) {
        //TODO: This returns the UTF8 name associated with this token.  (Is this method ever going to be useful?)
        return null;
    }

    public int newSet(int correlationId, CharSequence name) {
        assert (name.length() + 8 < configurator.getMaxMidAmble()) :
            "The maximum Set name length is 64. The requested Set name is " + name.length() + " characters in length.";

        // Create a new token for this name
        ByteBuffer bb = Charset.forName("UTF-8").encode(CharBuffer.wrap(name));
        byte[] midAmbleName = new byte[bb.remaining()];
        bb.get(midAmbleName);

        // Add 4 for partition Hash + 4 for UTF-8 length indicator
        int tokenLength = midAmbleName.length + 8;
        int newToken = token.getAndAdd(tokenLength);
        if (newToken > penultimateMidAmbleEntry) {
            reallocMidAmble(tokenLength, newToken);
        }
        // Put the Partition Hash for this Correlation ID in the first four bytes
        int bytePos = newToken;
        // ToDo: Create a real Partition hash to use here in place of 1
        int partitionHash = 1;
        bytePos = LittleEndianByteHelpers.writeInt32(partitionHash, bytePos, midAmbles);
        bytePos = LittleEndianByteHelpers.writeInt32(tokenLength, bytePos, midAmbles);
        System.arraycopy(midAmbleName, 0, midAmbles, bytePos, midAmbleName.length);

        // Put the position in the array in the top 16
        newToken <<= 16;
        // Put the length in the lower 16
        return newToken + tokenLength;
    }

    private synchronized void reallocMidAmble(int len, int newToken) {
        int checkToken = token.get();
        if ((checkToken == (newToken + len)) && (newToken > penultimateMidAmbleEntry)) {
            midAmbles = Arrays.copyOf(midAmbles, midAmbles.length * 2);
            penultimateMidAmbleEntry = midAmbles.length - (configurator.getMaxMidAmble());
        }
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

