package com.ociweb.hazelcast.stage;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

//import com.hazelcast.client.impl.protocol.util.Int2ObjectHashMap;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.ociweb.hazelcast.HZDataInput;
import com.ociweb.pronghorn.pipe.LittleEndianDataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RequestDecodeStageTest {

    private static final int BEGIN_FLAG = 128;    
    private static final int END_FLAG   = 64;
    private static final int EVENT_FLAG = 1; //This is the only flag passed on also populated by HZ
    
    
    private class CallbackCollector implements ResponseCallBack {

        public final int[] cArray;
        public final int[] tArray;
        public final int[] fArray;
        public final int[] pArray;
        public final byte[][] data;
        
        int instance;
        
        private CallbackCollector(int size) {
            cArray = new int[size];
            tArray = new int[size];
            fArray = new int[size];
            pArray = new int[size];
            data = new byte[size][];
            instance = 0;
        }
        
        @Override
        public void send(int correlationId, short type, short flags, int partitionId, HZDataInput reader) {
         
          cArray[instance] = correlationId;
          tArray[instance] = type;
          fArray[instance] = flags;
          pArray[instance] = partitionId;
         
          try {
              data[instance] = new byte[reader.available()];
              reader.read(data[instance]);
              instance++;
          } catch (IOException e) {
             throw new RuntimeException(e);
          }   
        }

        @Override
        public void testSend(String testMessage) {
            System.err.println("TestCollector.testSend received: " + testMessage);
        }
    }
    
    
    @Test    
    public void testDecoderAssemblyOfFragments() {
        
        int maxInputPipesCount = 48; //this is how many connections and therefore members of the cluster.
        int maxParts = 4;
        PipeConfig<RequestResponseSchema> config = new PipeConfig<RequestResponseSchema>(RequestResponseSchema.instance, 30, 5000);
        
        for(int i = 1;i<=maxInputPipesCount;i++) {
            for(int p = 1;p<=maxParts;p++) {
                testDecoderOfFragmentsInPipes(i, p, config);
            }
        }
    }

    private void testDecoderOfFragmentsInPipes(int inputCount, int parts, PipeConfig<RequestResponseSchema> config) {
        
        Pipe[] testPipes = new Pipe[inputCount];
        
        int iterations = 4;
        int x = iterations;
        while (--x>=0) {
        
            
            Pipe<RequestResponseSchema>[] inputFromConnection = buildPipesWithTestData(config, parts, testPipes);        
            
            int t = inputCount;
            
            while(--t>=0) {
                int contentRemaining = Pipe.contentRemaining(inputFromConnection[t]);
                assertEquals(parts*7,contentRemaining);
            }
            
            CallbackCollector collector = new CallbackCollector(inputCount);
            
            GraphManager gm = new GraphManager();
            RequestDecodeStage decodeStage = new RequestDecodeStage(gm , inputFromConnection, collector);
            
            //startup and run the stage
            decodeStage.startup();
            decodeStage.run();
           
            //confirm that the run consumed all the messages on all the input pipes
            t = inputCount;
            while(--t>=0) {
                Pipe.releaseAllBatchedReads(inputFromConnection[t]);
                int contentRemaining = Pipe.contentRemaining(inputFromConnection[t]);
                assertEquals("Iteration number "+x+" pipe "+t+" of "+inputCount,0, contentRemaining);
            }
            
            //validate the data with what was sent
            int i = inputCount;
            int c = 0;
            while (--i>=0) {
                byte[] testBlock = buildTestData(i);            
                int type = (i+1);
                int flags = 3 | BEGIN_FLAG | END_FLAG;//never fragmented.
                int corr = 100+i;
                int par  = 3+i;
                            
                assertEquals(type,  collector.tArray[c]);
                assertEquals(flags, collector.fArray[c]);
                assertEquals(corr,  collector.cArray[c]);
                assertEquals(par,  collector.pArray[c]);
                assertTrue(Arrays.equals(testBlock, collector.data[c]));
                c++;
                
            }
        }
    }

    private Pipe<RequestResponseSchema>[] buildPipesWithTestData(
            PipeConfig<RequestResponseSchema> config, int parts, Pipe<RequestResponseSchema>[] inputFromConnection) {
        int i = inputFromConnection.length;
               
        while (--i>=0) {
            if (null ==  inputFromConnection[i]) {//reuse so we can force a wrap on some tests.
                inputFromConnection[i] = new Pipe<RequestResponseSchema>(config);
                //init pipe for use
                inputFromConnection[i].initBuffers();
            }
            Pipe<RequestResponseSchema> p = inputFromConnection[i];
            
            assertEquals(0,Pipe.contentRemaining(p));
       
            byte[] testBlock = buildTestData(i);
            
            int j = parts;
            int type = (i+1)<<16;
            int flags = 3;
            int corr = 100+i;
            int par  = 3+i;
            int blockStep = testBlock.length/3;
            int blockStart = 0;
            
            while (--j>=0) { 
                
                Pipe.addMsgIdx(p, RequestResponseSchema.MSG_RESPONSE_1);
                Pipe.addIntValue(flags | type | (0==j?END_FLAG : ((parts-1)==j?BEGIN_FLAG:0)), p);
                Pipe.addIntValue(corr, p);
                Pipe.addIntValue(par,  p);
                int sourceLen = j>0?blockStep:testBlock.length-blockStart;
                Pipe.addByteArray(testBlock, blockStart, sourceLen, p);
                Pipe.publishWrites(p);
                
                blockStart+=blockStep;
            }        
        }
        return inputFromConnection;
    }


    private byte[] buildTestData(int i) {
        //populate with test data
        int testLen = i * 100;
        int k = testLen;
        byte[] testBlock = new byte[k];
        while (--k>=0) {
            testBlock[k] = (byte)(0x7F&k);
        }
        return testBlock;
    }

//    @Test
//    public void serailziationTest() {
//        //Mostly checking for compatibility.
//        LittleEndianDataInputBlobReader<RequestResponseSchema> reader = null;
//        
//        //TODO: need to use my own map for factories?
//        final Int2ObjectHashMap<DataSerializableFactory> factories = new Int2ObjectHashMap<DataSerializableFactory>();
//        
//        
//        try {
//            DataSerializable ds = null;
//            boolean isIdentified = reader.readBoolean();//byte !=0
//            if (isIdentified) {
//                
//                //what if the caller reads the values then does its own construction?
//                //   isIdentified?
//                //   getfactory
//                //   getid
//                //   do your own create
//                //    obj = factories.get(factoryId).create(id);
//                //   obj.readData(reader)
//                
//                //  obj = DataSerializable.decode(client, reader); //enable replacement.
//                
//                // for faster
//                //  obj = new Thing();
//                //  obj.readData(reader);
//                
//                int factoryId = reader.readInt();
//                final DataSerializableFactory dsf = factories.get(factoryId);//TODO: cache last.
//                if (dsf != null) {
//                    int id = reader.readInt();
//                    ds = dsf.create(id);//TODO: poly call and contains switch.
//                    if (ds != null) {
//                    } else {                    
//                        throw new UnsupportedOperationException();
//                    }
//                } else {
//                    throw new UnsupportedOperationException();
//                    
//                }
//                
//            } else {
//                String className = reader.readUTF();//string match on names of primitives then this class.
//                ds = ClassLoaderUtil.newInstance(reader.getClass().getClassLoader(), className);
//                
//            }
//            
//            //Must implement ObjectDataInput  in the reader for this, need new extending object
//            //ds.readData(reader);
//            //returns ds as new object
//            
//        
//        
//        } catch (IOException e) {
//           throw new RuntimeException();
//        } catch (Exception e) {
//            throw new RuntimeException();
//        }
//        
//        
//        
//        
//        
//        
//    }
    
    
    
}
