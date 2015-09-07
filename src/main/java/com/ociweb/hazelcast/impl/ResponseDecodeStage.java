package com.ociweb.hazelcast.impl;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ResponseDecodeStage extends PronghornStage {

    protected ResponseDecodeStage(GraphManager graphManager, Pipe input, Pipe output) {
        super(graphManager, input, output);
        // TODO Auto-generated constructor stub
    }

    //NOTE: consumer of these decoded messages could be the visitor reader, this would support everything with little work.
    //
    //   or consumer of these decoded messages could high level API code generated into classes? perhaps in the future.
    
    
    @Override
    public void run() {
        // TODO Auto-generated method stub
        
        
        //read every frame
        //look at the type, index into mapped script
        
        //expand into the fields for that type
        //if type not found report field guess and generate possible xml
        
        //unit test will cover all the methods and cause every type to be responded.
        
        //HighLevelAPI ->  BusMessages -> LowLevelEncode -> Bytes -> SocketSend
        //???              BusMessages <- LowLevelDecode <- Bytes <- SocketSend
        //Generic visitor
        //Generated high level proxy?
        //subscribe to correlationID?
        
        
    }

    private static int tryReadText(ByteBuffer targetBuffer, int pos, StringBuilder target, int frameLimit) { //TOOD: Must extract this into a much simpler method.
        
        int limit = frameLimit;
        int idx = pos+4;
        if (idx>limit) {
            return pos;
        }
        int tempBldrLength = target.length();
        int length = ConnectionStage.readInt32(targetBuffer, pos);
        if (idx+length<=limit) {
            //May be a text field must try the decode to find out.
                        
            byte[] rawBytes = targetBuffer.array();
            char badChar = 0xFFFD;
            long charAndPos = ((long)idx)<<32;
            long lim = ((long)idx+length)<<32;
    
            while (charAndPos<lim) {
                charAndPos = Pipe.decodeUTF8Fast(rawBytes, charAndPos, 0xFFFFFFFF); 
                char c = (char)charAndPos;
                if (badChar==c) {
                    break;//this was not text or badly encoded so we can not guess it was text.
                }
                target.append((char)charAndPos);
            }            
            return idx+length; 
        }
        //for good hygiene
        target.setLength(tempBldrLength);
        return pos; //is NOT a text field
    }

    private static void tryReadMessage(ByteBuffer targetBuffer, int pos, int frameLimit, StringBuilder workSpace) {
        long  accumLongValue = 0;
        int   accumIntValue = 0;
        short accumShortValue = 0;
        
        
        int shiftLong = 0;
        int shiftInt = 0;
        int shiftShort = 0;
        
        do {
            int oldPos;                             
            do {
                
                oldPos = pos;                            
                workSpace.setLength(0);
                pos = ResponseDecodeStage.tryReadText(targetBuffer, pos, workSpace, frameLimit);
                if (pos>oldPos) {
                    
                    accumLongValue = 0;
                    accumIntValue = 0;
                    accumShortValue = 0;
                    shiftLong = 0;
                    shiftInt = 0;
                    shiftShort = 0;                    
                    
                    System.err.println("Text:"+workSpace.toString());
                }
            } while (pos>oldPos);
            //we are done OR we have something that is NOT text
            if (pos<frameLimit) {
                long oneByte = targetBuffer.get(pos++);
                
                accumLongValue |= ((0xFF&oneByte)<<shiftLong);
                accumIntValue |= ((0xFF&oneByte)<<shiftInt);
                accumShortValue |= ((0xFF&oneByte)<<shiftShort);
                
                shiftLong += 8;
                shiftInt  += 8;
                shiftShort += 8;
                                                
                System.err.println("Byte:"+oneByte+
                                   (shiftShort==16 ? (" Short:"+accumShortValue) : "") +
                                   (shiftInt==32 ? (" Int:"+accumIntValue) : "") +
                                   (shiftLong==64 ? (" Long:"+accumLongValue) : ""));
                
                if (shiftLong >= 64) {
                    shiftLong = 0;
                    accumLongValue = 0;
                }
                
                if (shiftInt >= 32) {
                    shiftInt = 0;
                    accumIntValue = 0;
                }
                
                if (shiftShort >= 16) {
                    shiftShort = 0;
                    accumShortValue = 0;
                }
            } else {
                break;
            }
        } while (true);
    }

    
////  int correlationId = readInt32(targetBuffer, pos);
////  pos+=4;
////  
////  int partitionHash = readInt32(targetBuffer, pos);
////  pos+=4;
////  
////  int offset = 0xFF&targetBuffer.get(pos++);
////  offset |= (0xFF&targetBuffer.get(pos++))<<8;
////  assert(18==offset) : "No support for extended offset header";
//
//  
//  //TODO: use array to check if these Ids are known, same as server side;
//  
//  switch (taskId) {
//  
//  
//      case Integer.MAX_VALUE:
//          
//          
//          //for most task types we want assert(isAuthenticated);
//          
//      break;
//      default:
//          //Unknown response so document what we got.
//          System.err.println("Task:"+Integer.toHexString(taskId));
//        //  System.err.println("Correlation:"+correlationId);
//          //System.err.println("Partition:"+partitionHash);
//          //////////////
//          //Here there be dragons
//          //This code guesses at field structure
//          //////////////
//          tryReadMessage(targetBuffer, pos, frameStop, workSpace);                               
//          
//  }
//  

//
//  
//  //what are we sending back?
//  // CorrelationId
//  // Payload
//  
//  
//  
//  //just the payload because the corelation must match to what the caller sent?
//  //security/effeciencty balance?
    
}
