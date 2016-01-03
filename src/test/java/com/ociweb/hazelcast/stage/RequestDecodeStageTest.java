package com.ociweb.hazelcast.stage;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.LittleEndianDataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RequestDecodeStageTest {

    @Test    
    public void testSomething() {
        
        int inputCount = 3;
        PipeConfig<RequestResponseSchema> config = new PipeConfig<RequestResponseSchema>(RequestResponseSchema.instance, 10, 1000);
        
        int i = inputCount;
        Pipe<RequestResponseSchema>[] inputFromConnection = new Pipe[i];        
        while (--i>=0) {
            inputFromConnection[i] = new Pipe<RequestResponseSchema>(config);
            inputFromConnection[i].initBuffers();
        }
        
        ResponseCallBack callBack = new ResponseCallBack() {
            
            @Override
            public void send(int correlationId, int type, int partitionId,  LittleEndianDataInputBlobReader<RequestResponseSchema> reader) {
                // TODO Auto-generated method stub
                
            }
        };

        GraphManager gm = new GraphManager();
        RequestDecodeStage decodeStage = new RequestDecodeStage(gm , inputFromConnection, callBack);
        
        //TOOD: put data in input pipes
        
       decodeStage.startup();
      // decodeStage.run();
       
       //TODO: confirm the expected callbacks
       
        //TODO: need to integrate the identifiable serialziation
        
                
    }
    
    
    
}
