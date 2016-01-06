package com.ociweb.hazelcast.stage;

import com.ociweb.hazelcast.stage.HazelcastConfigurator;
import com.ociweb.hazelcast.HazelcastClientConfig;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HazelcastClient {

    private static int maximumLengthOfHzVariableFields = 1024;
    private static int minimumNumberOfHzOutgoingFragments = 2;
    private static Pipe<HazelcastRequestsSchema> requestPipe;

    private static int maximumLengthOfRawVariableFields = 2048;
    private static int minimumNumberOfRawOutgoingFragments = 5;
    private static Pipe<RawDataSchema> encoderToConnectionPipe;
    private static Pipe<RequestResponseSchema> connectionToDecoderPipe;

    private GraphManager gm = new GraphManager();
    private HazelcastConfigurator configurator;

    private AtomicInteger token = new AtomicInteger(0);
    private byte[] midAmbles;
    private int penultimateMidAmbleEntry = 0;


    // ToDo: This is a temp cid location for the names until the tokens are put into play.
    private Map<Integer, CharSequence> names = new HashMap<>(10);

    public HazelcastClient(HazelcastClientConfig config, ResponseCallBack callBack) {

        configurator = new HazelcastConfigurator();
        // Future: Use this to circumvent the conversion of the name to UTF-8 for every call.
        midAmbles = new byte[(configurator.getMaxMidAmble() * 50)];
        penultimateMidAmbleEntry = midAmbles.length - (configurator.getMaxMidAmble());

        HazelcastClient.buildClientGraph(gm, configurator, callBack);
        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        scheduler.startup();

        // ToDo: TEMP -- remove this later, but until this is running ...
        scheduler.awaitTermination(3000, TimeUnit.SECONDS);
    }

    public static void buildClientGraph(GraphManager gm, HazelcastConfigurator configurator, ResponseCallBack callBack) {
        PipeConfig<HazelcastRequestsSchema> hzProtocolConfig =
            new PipeConfig<>(HazelcastRequestsSchema.instance, minimumNumberOfHzOutgoingFragments, maximumLengthOfHzVariableFields);

        PipeConfig<RawDataSchema> rawDataConfig =
            new PipeConfig<>(RawDataSchema.instance, minimumNumberOfRawOutgoingFragments, maximumLengthOfRawVariableFields);

        PipeConfig<RequestResponseSchema> responseConfig =
            new PipeConfig<>(RequestResponseSchema.instance, minimumNumberOfRawOutgoingFragments, maximumLengthOfRawVariableFields);

        requestPipe = new Pipe<>(hzProtocolConfig);
        for (int pipeNumber = 0; pipeNumber < configurator.getNumberOfConnectionStages(); pipeNumber++) {
            configurator.encoderToConnectionPipes[pipeNumber] = new Pipe<RawDataSchema>(rawDataConfig);
            configurator.connectionToDecoderPipes[pipeNumber] = new Pipe<RequestResponseSchema>(responseConfig);
        }

        new RequestEncodeStage(gm, requestPipe, configurator.encoderToConnectionPipes, configurator);
        for (int pipeNumber = 0; pipeNumber < configurator.getNumberOfConnectionStages(); pipeNumber++) {
            configurator.connectionStage[pipeNumber] =
                new ConnectionStage(gm, configurator.encoderToConnectionPipes[pipeNumber], configurator.connectionToDecoderPipes[pipeNumber], configurator);
        }
        new RequestDecodeStage(gm, configurator.connectionToDecoderPipes, callBack);

        MonitorConsoleStage.attach(gm);
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
        int returnToken = newToken + tokenLength;
        names.put(new Integer(returnToken), name);
        return returnToken;
    }

    public Pipe<HazelcastRequestsSchema> getRequestPipe() {
        return requestPipe;
    }


    public byte[] getMidAmbles() {
        return midAmbles;
    }

    //TODO: This returns the UTF8 name associated with this token. Use this until tokens are implemented.
    public CharSequence getName(int token) {
        return names.get(token);
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

