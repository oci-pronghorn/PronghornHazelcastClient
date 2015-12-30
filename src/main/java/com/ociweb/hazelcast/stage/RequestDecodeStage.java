package com.ociweb.hazelcast.stage;

import java.io.IOException;

import com.ociweb.hazelcast.stage.util.LittleEndianByteHelpers;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.LittleEndianDataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * This class reads a response from the Hazelcast cluster in RawDataFormat. It decodes the response into a Correlation ID and
 * a HazelcastResponse and invokes the Callback associated with the Correlation ID.
 */

public class RequestDecodeStage extends PronghornStage {
    
    private Pipe<RawDataSchema>[] inputFromConnection;    
    private LittleEndianDataInputBlobReader<RawDataSchema>[] readers;
    private static final int msgSize = RawDataSchema.FROM.fragDataSize[RawDataSchema.MSG_CHUNKEDSTREAM_1];
    
    public RequestDecodeStage(GraphManager gm, Pipe<RawDataSchema>[] inputFromConnection, HazelcastConfigurator configurator) {
        super(gm, inputFromConnection, NONE);
        this.inputFromConnection = inputFromConnection;
    }

    @Override
    public void startup() {
        
        int j = inputFromConnection.length;
        readers = new LittleEndianDataInputBlobReader[j];
        while (--j>=0) {
            readers[j]=new LittleEndianDataInputBlobReader<RawDataSchema>(inputFromConnection[j]);
        }
    }

    @Override
    public void run() {
        
        int j = inputFromConnection.length;
        int c;
        do {
            c = 0;
            while (--j>=0) {
                c += readFromPipe(inputFromConnection[j],readers[j]);
            }
        } while (c>0);//keep going until we find that no pipes have any data
        
    }

    private int readFromPipe(Pipe<RawDataSchema> pipe, LittleEndianDataInputBlobReader<RawDataSchema> reader) {
        int c = 0;
        while (Pipe.hasContentToRead(pipe)) { //keep going while this pipe has data
            
            int msgIdx = Pipe.takeMsgIdx(pipe);
            assert(RawDataSchema.MSG_CHUNKEDSTREAM_1 == msgIdx) : "Only one message template is supported";
            
            //TODO:B, after reading the contextID should hash and send to stage to make the threaded call backs without contention.
            
            reader.openLowLevelAPIField();
            try {
                int frameSize     = reader.readInt();
                int version       = reader.readByte();
                int flags         = reader.readByte();
                int type          = reader.readShort();
                int correlationId = reader.readInt();
                int parititinoId  = reader.readInt();
                int dataOffset    = reader.readShort();
                                
                reader.skip(dataOffset-18);
                
                //the reader is now positioned to read the payaload.
                
                //TODO: add the coid for in flight check.
                
                //correlation id for assmbly of the pipe.
                
                
                
                
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            
            
            
            Pipe.confirmLowLevelRead(pipe, msgSize);
            Pipe.readNextWithoutReleasingReadLock(pipe);
        }
        
        return c;
    }
    

}
