package com.ociweb.hazelcast;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.hazelcast.stage.*;
import com.ociweb.hazelcast.stage.util.InetSocketAddressImmutable;
import com.ociweb.hazelcast.stage.util.LittleEndianByteHelpers;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HazelcastConfigurator carries all the configuration information used by the various
 * stages of a Hazelcast Pronghorn client.
 */
public class HazelcastConfigurator {

    private final static Logger log = LoggerFactory.getLogger(RequestsProxy.class);

    private AtomicInteger token = new AtomicInteger(0);
    private byte[] midAmbles;
    private int penultimateMidAmbleEntry = 0;

    private int maximumLengthOfHzVariableFields = 1024;
    private int minimumNumberOfHzOutgoingFragments = 2;

    private int maximumLengthOfRawVariableFields = 2048;
    private int minimumNumberOfRawOutgoingFragments = 5;

    private int numberOfConnectionStages = 1;

    // ToDo: This is a temp id location serving as a proxy for the midambles names until the tokens are put into play.
    private Map<Integer, CharSequence> names = new HashMap<>(10);

    // This represents the max length of name (64 to start with) + 4 byte partition hash + 4 byte UTF vli
    private int maxMidAmbleLength = 72;
    private CharSequence[] tokenNames = new CharSequence[512];

    protected Pipe<HazelcastRequestsSchema> requestPipe;
    protected Pipe[] encoderToConnectionPipes = new Pipe[numberOfConnectionStages];
    protected Pipe[] connectionToDecoderPipes = new Pipe[numberOfConnectionStages];
    protected ConnectionStage[] connectionStage = new ConnectionStage[numberOfConnectionStages];

    private ThreadPerStageScheduler scheduler;

    private GraphManager gm = new GraphManager();
    private ResponseCallBack callBack;
    private RequestsProxy requestsProxy;

    public HazelcastConfigurator() {
        this(null, null);
    }

    public HazelcastConfigurator(String configurationFileName, ResponseCallBack callBack) {
        Path configFilePath;
        if (configurationFileName == null) {
            configFilePath = FileSystems.getDefault().getPath("~", ".hz", "configFile");
        } else {
            configFilePath = FileSystems.getDefault().getPath(configurationFileName);
        }

        // Future: Use this to circumvent the conversion of the name to UTF-8 for every call.
        midAmbles = new byte[(getMaxMidAmble() * 50)];
        penultimateMidAmbleEntry = midAmbles.length - getMaxMidAmble();

        this.callBack = callBack;
        buildClientGraph();
        scheduler = new ThreadPerStageScheduler(gm);
        scheduler.startup();
    }

    public void buildClientGraph() {
        PipeConfig<HazelcastRequestsSchema> hzProtocolConfig = new PipeConfig<>(HazelcastRequestsSchema.instance, minimumNumberOfHzOutgoingFragments, maximumLengthOfHzVariableFields);
        PipeConfig<RawDataSchema> rawDataConfig = new PipeConfig<>(RawDataSchema.instance, minimumNumberOfRawOutgoingFragments, maximumLengthOfRawVariableFields);
        PipeConfig<RequestResponseSchema> responseConfig = new PipeConfig<>(RequestResponseSchema.instance, minimumNumberOfRawOutgoingFragments, maximumLengthOfRawVariableFields);

        requestPipe = new Pipe<>(hzProtocolConfig);
        for (int pipeNumber = 0; pipeNumber < getNumberOfConnectionStages(); pipeNumber++) {
            encoderToConnectionPipes[pipeNumber] = new Pipe<RawDataSchema>(rawDataConfig);
            connectionToDecoderPipes[pipeNumber] = new Pipe<RequestResponseSchema>(responseConfig);
        }

        // The input source.  This proxy will be used by the client API to send HZ requests to the encoder
        requestsProxy = new RequestsProxy(gm, requestPipe);
        GraphManager.addNota(gm, GraphManager.UNSCHEDULED, GraphManager.UNSCHEDULED, requestsProxy);

        // The Encoder will take the requests, convert them to the BinaryClientProtocol and send the stream to the correct Connection Stage.
        new RequestEncodeStage(gm, requestPipe, encoderToConnectionPipes, this);

        // The connection stage handles the sends and receives of the Hazelcast server communications
        for (int pipeNumber = 0; pipeNumber < numberOfConnectionStages; pipeNumber++) {
            connectionStage[pipeNumber] = new ConnectionStage(gm, encoderToConnectionPipes[pipeNumber], connectionToDecoderPipes[pipeNumber], this);
//            configurator.connectionStage[pipeNumber] = new ConsoleJSONDumpStage<RawDataSchema>(gm, configurator.encoderToConnectionPipes[pipeNumber], System.out);
        }

//        new ConsoleJSONDumpStage<RawDataSchema>(gm, configurator.connectionToDecoderPipes[0], System.err);
        // The Decoder receives the responses from the server and sends them on to the callback provided by the client
        new RequestDecodeStage(gm, connectionToDecoderPipes, callBack);

        MonitorConsoleStage.attach(gm);
        log.debug("Built graph");
    }


    protected int getNumberOfConnectionStages() {
        return numberOfConnectionStages;
    }

    public InetSocketAddress buildInetSocketAddress(int stageId) {
       return new InetSocketAddressImmutable("127.0.0.1", 5701);
    }

    public CharSequence getUUID(int stageId) {
        return "";
    }

    public CharSequence getOwnerUUID(int stageId) {
        return "";
    }

    public boolean isCustomAuth() {
        return false;
    }

    public byte[] getCustomCredentials() {
        return null;
    }

    public CharSequence getUserName(int stageId) {
        return "dev"; //default value
    }

    public CharSequence getPassword(int stageId) {
        return "dev-pass"; //default value
    }

    public int maxClusterSize() {
        return 271;
    }

    public int getHashKeyForRingId(int ringId) {
        //can only return this after the connection stages register which node they have and which ring is coming in.
        // TODO Auto-generated method stub
        return 0;
    }

    public int getSetToken(HazelcastClient client, int correlationId, CharSequence name) {
        assert (name.length() + 8 < getMaxMidAmble()) :
            "The maximum Set name length is 64. The requested Set name is " + name.length() + " characters in length.";

        // ToDo: In actual fact, we should get a response from this before doing the Partitions (next line)
        if (!ClientProxyHelper.createProxy(client, 1, "cas_work", "hz:impl:setService")) {
            log.error("Died during createProxy");
            return -1;
        }
        log.debug("Built Proxy");

        if (!ClientProxyHelper.getPartitions(client, 2)) {
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
        int returnToken = newToken + tokenLength; //TODO: This highly dangerous and only works if you know tokenLength will never be negative and will always be less than 32K
        names.put(new Integer(returnToken), name); ///TODO: This is not the right data structure, revisit.
        log.debug("Finished new Set creation for " + name);
        return returnToken;
    }

    private synchronized void reallocMidAmble(int len, int newToken) {
        int checkToken = token.get();
        if ((checkToken == (newToken + len)) && (newToken > penultimateMidAmbleEntry)) {
            midAmbles = Arrays.copyOf(midAmbles, midAmbles.length * 2);
            penultimateMidAmbleEntry = midAmbles.length - getMaxMidAmble();
        }
    }

    public int getMaxMidAmble() {
        return maxMidAmbleLength;
    }

    public byte[] getMidAmbles() {
        return midAmbles;
    }

    //TODO: This returns the UTF8 name associated with this token. Use this until tokens are implemented.
    public CharSequence getName(int token) {
        return names.get(token);
    }

    public void setNameForToken(int token, CharSequence name) {
        tokenNames[token] = name;
    }

    public CharSequence getNameForToken(int token) {
        return tokenNames[token];
    }

    public int getMaximumLengthOfHzVariableFields() {
        return maximumLengthOfHzVariableFields;
    }

    public void setMaximumLengthOfHzVariableFields(int maximumLengthOfHzVariableFields) {
        this.maximumLengthOfHzVariableFields = maximumLengthOfHzVariableFields;
    }

    public int getMinimumNumberOfHzOutgoingFragments() {
        return minimumNumberOfHzOutgoingFragments;
    }

    public void setMinimumNumberOfHzOutgoingFragments(int minimumNumberOfHzOutgoingFragments) {
        this.minimumNumberOfHzOutgoingFragments = minimumNumberOfHzOutgoingFragments;
    }

    public int getMaximumLengthOfRawVariableFields() {
        return maximumLengthOfRawVariableFields;
    }

    public void setMaximumLengthOfRawVariableFields(int maximumLengthOfRawVariableFields) {
        this.maximumLengthOfRawVariableFields = maximumLengthOfRawVariableFields;
    }

    public int getMinimumNumberOfRawOutgoingFragments() {
        return minimumNumberOfRawOutgoingFragments;
    }

    public void setMinimumNumberOfRawOutgoingFragments(int minimumNumberOfRawOutgoingFragments) {
        this.minimumNumberOfRawOutgoingFragments = minimumNumberOfRawOutgoingFragments;
    }

    public void setNumberOfConnectionStages(int numberOfConnectionStages) {
        this.numberOfConnectionStages = numberOfConnectionStages;
    }

    public void setCallBack(ResponseCallBack callBack) {
        this.callBack = callBack;
    }

    public RequestsProxy getRequestsProxy() {
        return requestsProxy;
    }

/* TODO: merge this as docs above
HazelcastConfigurator conf = new HazelcastConfigurator() {

    public InetSocketAddress buildInetSocketAddress(int stageId) {
        return new InetSocketAddressImmutable("127.0.0.1",port);
     }

    public CharSequence getUUID(int stageId) {
        return "ThisIsMe";
    }

    public CharSequence getOwnerUUID(int stageId) {
        return "ThisIsNotMe";
    }

    public CharSequence getUserName(int stageId) {
        return "dev"; //default value
    }

    public CharSequence getPassword(int stageId) {
        return "dev-pass"; //default value
    }


    public boolean isCustomAuth() {
        return false;

    }
};
*/

}
