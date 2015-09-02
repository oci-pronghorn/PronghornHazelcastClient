package com.ociweb.hazelcast.impl;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class GraphBuilder {

        
    public static GraphManager build(Configurator config) {
        GraphManager gm = new GraphManager();
    
        RingBufferConfig rawBytesRingConfig = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES, 4000, 2048);
        
        
        RingBuffer hazelcastRequestMessages = new RingBuffer(rawBytesRingConfig);
        
        int maxClusterSize = config.maxClusterSize();
        
        RingBuffer[] wireMessagesOutputPipe = new RingBuffer[maxClusterSize];
        RingBuffer[] wireMessagesInputPipe = new RingBuffer[maxClusterSize];
        
        int i = maxClusterSize;
        while (--i>=0) {
        
            wireMessagesOutputPipe[i] = new RingBuffer(rawBytesRingConfig);
            wireMessagesInputPipe[i] = new RingBuffer(rawBytesRingConfig);
                                
            ConnectionStage cStage = new ConnectionStage(gm, wireMessagesOutputPipe[i],wireMessagesInputPipe[i], config);
        
        }
        
        RequestEnocdeStage mbs = new RequestEnocdeStage(gm,hazelcastRequestMessages, wireMessagesOutputPipe, config);
        
        
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
