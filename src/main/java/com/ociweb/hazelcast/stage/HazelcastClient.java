package com.ociweb.hazelcast.stage;

import com.ociweb.hazelcast.HazelcastClientConfig;
import com.ociweb.hazelcast.stage.util.LittleEndianByteHelpers;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HazelcastClient {

    private final static Logger log = LoggerFactory.getLogger(HazelcastClient.class);

    private int maximumLengthOfHzVariableFields = 1024;
    private int minimumNumberOfHzOutgoingFragments = 2;
    private Pipe<HazelcastRequestsSchema> requestPipe;

    private int maximumLengthOfRawVariableFields = 2048;
    private int minimumNumberOfRawOutgoingFragments = 5;

    private ThreadPerStageScheduler scheduler;

    private GraphManager gm = new GraphManager();
    private HazelcastConfigurator configurator;
    private RequestsProxy requestsProxy;

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

        buildClientGraph(gm, configurator, callBack);
        scheduler = new ThreadPerStageScheduler(gm);
        scheduler.startup();
        log.debug("Finished Constructor");
    }

    public void buildClientGraph(GraphManager gm, HazelcastConfigurator configurator, ResponseCallBack callBack) {
        PipeConfig<HazelcastRequestsSchema> hzProtocolConfig = new PipeConfig<>(HazelcastRequestsSchema.instance, minimumNumberOfHzOutgoingFragments, maximumLengthOfHzVariableFields);
        PipeConfig<RawDataSchema> rawDataConfig = new PipeConfig<>(RawDataSchema.instance, minimumNumberOfRawOutgoingFragments, maximumLengthOfRawVariableFields);
        PipeConfig<RequestResponseSchema> responseConfig = new PipeConfig<>(RequestResponseSchema.instance, minimumNumberOfRawOutgoingFragments, maximumLengthOfRawVariableFields);

        requestPipe = new Pipe<>(hzProtocolConfig);
        for (int pipeNumber = 0; pipeNumber < configurator.getNumberOfConnectionStages(); pipeNumber++) {
            configurator.encoderToConnectionPipes[pipeNumber] = new Pipe<RawDataSchema>(rawDataConfig);
            configurator.connectionToDecoderPipes[pipeNumber] = new Pipe<RequestResponseSchema>(responseConfig);
        }

        // The input source.  This proxy will be used by the client API to send HZ requests to the encoder
        requestsProxy = new RequestsProxy(gm, requestPipe);
        GraphManager.addNota(gm, GraphManager.UNSCHEDULED, GraphManager.UNSCHEDULED, requestsProxy);

        // The Encoder will take the requests, convert them to the BinaryClientProtocol and send the stream to the correct Connection Stage.
        new RequestEncodeStage(gm, requestPipe, configurator.encoderToConnectionPipes, configurator);

        // The connection stage handles the sends and receives of the Hazelcast server communications
        for (int pipeNumber = 0; pipeNumber < configurator.getNumberOfConnectionStages(); pipeNumber++) {
            configurator.connectionStage[pipeNumber] = new ConnectionStage(gm, configurator.encoderToConnectionPipes[pipeNumber], configurator.connectionToDecoderPipes[pipeNumber], configurator);
//            configurator.connectionStage[pipeNumber] = new ConsoleJSONDumpStage<>(gm, configurator.encoderToConnectionPipes[pipeNumber], System.err);
        }

        // The Decoder receives the responses from the server and sends them on to the callback provided by the client
        new RequestDecodeStage(gm, configurator.connectionToDecoderPipes, callBack);

        MonitorConsoleStage.attach(gm);
        log.debug("Built graph");
    }

    public void stopScheduler() {
//        scheduler.awaitTermination(3, TimeUnit.SECONDS);
    }

    public int newSet(HazelcastClient client, int correlationId, CharSequence name) {
        assert (name.length() + 8 < configurator.getMaxMidAmble()) :
            "The maximum Set name length is 64. The requested Set name is " + name.length() + " characters in length.";

        // ToDo: In actual fact, we should get a response from this before doing the Partitions (next line)
        if (!Client.createProxy(client, 1, "cas_work", "\"hz:impl:setService")) {
            log.error("Died during createProxy");
            return -1;
        }
        log.debug("Built Proxy");
        if (!Client.getPartitions(client, 2)) {
            log.error("Died when getting partitions");
            return -1;
        }
        log.debug("Asked for partitions");

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
        log.debug("Finished new Set creation for " + name);
        return returnToken;
    }

    public RequestsProxy getRequestsProxy() {
        return requestsProxy;
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
        public static boolean createProxy(HazelcastClient client, int correlationId, CharSequence name, CharSequence serviceName) {
            if (client.getRequestsProxy().tryWriteFragment(0x0)) {
                return writeProxyInfo(client, correlationId, name, serviceName);
            } else {
                return false;
            }
        }

        public static boolean destroyProxy(HazelcastClient client, int correlationId, CharSequence name, CharSequence serviceName) {
            if (client.getRequestsProxy().tryWriteFragment(0x6)) {
                return writeProxyInfo(client, correlationId, name, serviceName);
            } else {
                return false;
            }
        }


        public static boolean getPartitions(HazelcastClient client, int correlationId) {
            if (client.getRequestsProxy().tryWriteFragment(0xc)) {
                client.getRequestsProxy().writeInt(0x1, correlationId);
                client.getRequestsProxy().writeInt(0x2, -1);
                client.getRequestsProxy().publishWrites();
                return true;
            } else {
                return false;
            }
        }

        private static boolean writeProxyInfo(HazelcastClient client, int correlationId, CharSequence name, CharSequence serviceName) {
            client.getRequestsProxy().writeInt(0x1, correlationId);
            client.getRequestsProxy().writeInt(0x2, -1);
            client.getRequestsProxy().writeUTF8(0x1400003, name);
            client.getRequestsProxy().writeUTF8(0x1400005, serviceName);
            client.getRequestsProxy().publishWrites();
            return true;
        }
    }
}

