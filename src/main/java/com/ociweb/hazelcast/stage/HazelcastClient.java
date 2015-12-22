package com.ociweb.hazelcast.stage;

import com.ociweb.hazelcast.HazelcastClientConfig;
import com.ociweb.hazelcast.stage.HazelcastConfigurator;
import com.ociweb.hazelcast.stage.ConnectionStage;
import com.ociweb.hazelcast.stage.HazelcastRequestsSchema;
import com.ociweb.hazelcast.stage.RequestEncodeStage;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

import java.util.concurrent.TimeUnit;

public class HazelcastClient {

    private int maximumLengthOfHzVariableFields = 1024;
    private int minimumNumberOfHzOutgoingFragments = 2;
    private static Pipe<HazelcastRequestsSchema> requestPipe;
    private static Pipe<HazelcastRequestsSchema> responsePipe;

    private int maximumLengthOfRawVariableFields = 2048;
    private int minimumNumberOfRawOutgoingFragments = 1;
    private static Pipe<RawDataSchema> encoderToConnectionPipe;
    private static Pipe<RawDataSchema> connectionToDecoderPipe;
    private static HazelcastConfigurator configurator;

    GraphManager gm = new GraphManager();

    public HazelcastClient(HazelcastClientConfig config) {

        // ToDo: Need to adjust the HazelcastConfigurator to actually configure waht is necessary; this works for now.
        configurator = new HazelcastConfigurator();

        PipeConfig<HazelcastRequestsSchema> hzProtocolConfig =
            new PipeConfig<>(HazelcastRequestsSchema.instance, minimumNumberOfHzOutgoingFragments, maximumLengthOfHzVariableFields);
        requestPipe = new Pipe<>(hzProtocolConfig);
        responsePipe = new Pipe<>(hzProtocolConfig);


        PipeConfig<RawDataSchema> rawDataConfig =
            new PipeConfig<>(RawDataSchema.instance, minimumNumberOfRawOutgoingFragments, maximumLengthOfRawVariableFields);

        for (int pipeNumber = 1; pipeNumber <= configurator.getNumberOfConnectionStages(); pipeNumber++) {
            configurator.encoderToConnectionPipes[pipeNumber] = new Pipe<>(rawDataConfig);
            configurator.connectionToDecoderPipes[pipeNumber] = new Pipe<>(rawDataConfig);
            configurator.connectionStage[pipeNumber] = new ConnectionStage(gm,
                configurator.encoderToConnectionPipes[pipeNumber], configurator.connectionToDecoderPipes[pipeNumber], configurator);
        }

        new RequestEncodeStage(gm, requestPipe,  configurator.encoderToConnectionPipes, configurator);
        new RequestDecodeStage(gm, configurator.connectionToDecoderPipes, configurator);

        MonitorConsoleStage.attach(gm);

        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
        scheduler.startup();

        // ToDo: TEMP -- remove this later, but for now (until running) ...
        scheduler.awaitTermination(300, TimeUnit.SECONDS);
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

