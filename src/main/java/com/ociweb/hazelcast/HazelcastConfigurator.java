package com.ociweb.hazelcast;

import com.ociweb.hazelcast.stage.ConnectionStage;
import com.ociweb.hazelcast.stage.HazelcastRequestsSchema;
import com.ociweb.hazelcast.stage.RequestDecodeStage;
import com.ociweb.hazelcast.stage.RequestEncodeStage;
import com.ociweb.hazelcast.stage.RequestResponseSchema;
import com.ociweb.hazelcast.stage.RequestsProxy;
import com.ociweb.hazelcast.stage.ResponseCallBack;
import com.ociweb.hazelcast.stage.util.InetSocketAddressImmutable;
import com.ociweb.hazelcast.stage.util.LittleEndianByteHelpers;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * The HazelcastConfigurator carries all the configuration information used by the various
 * stages of a Hazelcast Pronghorn client.
 */
public class HazelcastConfigurator {

    private final static Logger log = LoggerFactory.getLogger(RequestsProxy.class);


    private int maximumLengthOfHzVariableFields = 1024;
    private int minimumNumberOfHzOutgoingFragments = 2;

    private int maximumLengthOfRawVariableFields = 2048;
    private int minimumNumberOfRawOutgoingFragments = 5;

    private int numberOfConnectionStages = 1;

    protected Pipe<HazelcastRequestsSchema> requestPipe;
    protected Pipe[] encoderToConnectionPipes;
    protected Pipe[] connectionToDecoderPipes;
    protected ConnectionStage[] connectionStages;
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

        processConfigurationFile(configFilePath);

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

        encoderToConnectionPipes = new Pipe[numberOfConnectionStages];
        connectionToDecoderPipes = new Pipe[numberOfConnectionStages];
        for (int pipeNumber = 0; pipeNumber < numberOfConnectionStages; pipeNumber++) {
            encoderToConnectionPipes[pipeNumber] = new Pipe<RawDataSchema>(rawDataConfig);
            connectionToDecoderPipes[pipeNumber] = new Pipe<RequestResponseSchema>(responseConfig);
        }

        // The input source.  This proxy will be used by the client API to send HZ requests to the encoder
        requestsProxy = new RequestsProxy(gm, requestPipe);
        GraphManager.addNota(gm, GraphManager.UNSCHEDULED, GraphManager.UNSCHEDULED, requestsProxy);

        // The Encoder will take the requests, convert them to the BinaryClientProtocol and send the stream to the correct Connection Stage.
        new RequestEncodeStage(gm, requestPipe, encoderToConnectionPipes, this);

        // The connection stage handles the sends and receives of the Hazelcast server communications
        connectionStages = new ConnectionStage[numberOfConnectionStages];
        for (int pipeNumber = 0; pipeNumber < numberOfConnectionStages; pipeNumber++) {
            connectionStages[pipeNumber] = new ConnectionStage(gm, encoderToConnectionPipes[pipeNumber], connectionToDecoderPipes[pipeNumber], this);
//            configurator.connectionStages[pipeNumber] = new ConsoleJSONDumpStage<RawDataSchema>(gm, configurator.encoderToConnectionPipes[pipeNumber], System.out);
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

    private void processConfigurationFile(Path configFilePath) {
        // ToDo:  open the configFilePath, read the properties, set the configuration items.
    }

/* TODO: These will be configuration items set in the file, after setting in file,
        either add as config setters or delete.
HazelcastConfigurator conf = new HazelcastConfigurator() {

    public InetSocketAddress buildInetSocketAddress(int stageId) {
        return new InetSocketAddressImmutable("127.0.0.1",port);
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
