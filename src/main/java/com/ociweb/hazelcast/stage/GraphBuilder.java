package com.ociweb.hazelcast.stage;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class GraphBuilder {


    public static GraphManager build(Configurator config) {
        GraphManager gm = new GraphManager();

        PipeConfig rawBytesRingConfig = new PipeConfig(FieldReferenceOffsetManager.RAW_BYTES, 4000, 2048);


        Pipe hazelcastRequestMessages = new Pipe(rawBytesRingConfig);

        int maxClusterSize = config.maxClusterSize();

        Pipe[] wireMessagesOutputPipe = new Pipe[maxClusterSize];
        Pipe[] wireMessagesInputPipe = new Pipe[maxClusterSize];

        int i = maxClusterSize;
        while (--i>=0) {

            wireMessagesOutputPipe[i] = new Pipe(rawBytesRingConfig);
            wireMessagesInputPipe[i] = new Pipe(rawBytesRingConfig);

            ConnectionStage cStage = new ConnectionStage(gm, wireMessagesOutputPipe[i],wireMessagesInputPipe[i], config);

        }

        RequestEncodeStage mbs = new RequestEncodeStage(gm,hazelcastRequestMessages, wireMessagesOutputPipe, config);


        //optional feature (from pronghorn)
        //MergeStage
        //HazelcastAPI hz = new HazelcastAPI(gm, inputRingFrmCon, outputRingToMessageBuilder)



        /*
         *  Init with max connections for max size of cluster, will error out at runtime if there are too many.
         *  New decorator to never schedule stages.
         *
         *  Membership Management
         *
         *  based on hash will decide which queue the data goes on so calling code need not care.
         *  MessageBuilderStage - creates proxys and messages for modification of proxies
         *          IN: NAME, OP_ID, OTHER FIELDS in TemplateXML for each message
         *          OUT*: fully formed packets to the right member
         *
         *
         *   parts can be assmebled in diffeent orders for different reasons, TODO: provide mutliple graph builders.
         *    MSGBld -> conStage -> RoundRobinMerge ->  OptionalCorrelationRouter ->  optionalConsumer/requesterBaseClass
         *           -> conStage ->                                               ->
         *           -> conStage ->                                               ->
         *
         *  Base class must have methods for doing each of the operations, at least one for generic building at first.
         *
         *
         *
         */



        return gm;
    }


}
