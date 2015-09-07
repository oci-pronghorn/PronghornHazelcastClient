package com.ociweb.hazelcast.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ConnectionStage extends PronghornStage {

    private final static Logger log = LoggerFactory.getLogger(ConnectionStage.class);
    
    private final Pipe inputMessagesToSend; 
    private final Pipe outputMessagesRecieved;
    private final Configurator conf;
        
    private final long timeLimitMS = 1000;//TODO: where to set this ping?
    
    private boolean isAuthenticated = false;
    private long touched;
    
    private final static byte BIT_FLAG_START = (byte)0x80;
    private final static byte BIT_FLAG_END = (byte)0x40;
    
    private InetSocketAddress addr;
    private SocketChannel channel;
    private ByteBuffer initializerData;
    private ByteBuffer pingData;
    private ByteBuffer lengthData;
    
    private ByteBuffer[] pendingWriteBuffers;

    private ByteBuffer inputSocketBuffer;

    private static final int msgIdx = 0;
    private static final int msgSize = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[msgIdx]; //we only send one kind of message (packets to be sent)
    
    
    private StringBuilder authResponse = new StringBuilder(128);        
    private int authAddrLength;
    private int authUUIDLength;
    private int authUUIDLengthOwner;
    
    
    protected ConnectionStage(GraphManager graphManager, Pipe input, Pipe output, Configurator conf) {
        super(graphManager, input, output);
        this.inputMessagesToSend = input;
        this.outputMessagesRecieved = output;
        this.conf = conf;
        
        assert(Pipe.from(inputMessagesToSend).equals(FieldReferenceOffsetManager.RAW_BYTES)) : "Expected simple raw bytes for input.";
    }
    
    @Override
    public void startup() {        
        addr = conf.buildInetSocketAddress(this.stageId); //may also get called later if this node should go down.
        buildNewChannel();
        
        pendingWriteBuffers = new ByteBuffer[3];
        
        CharSequence uuid      = conf.getUUID(this.stageId);
        CharSequence uuidOwner = conf.getOwnerUUID(this.stageId);

        int bytesMaxEstimate = 100+ 3+18+3+((uuid.length()+uuidOwner.length())*6);
        
        byte[] initBytes;
        int idx = 0;
        int lenIdx;
        int tLen;
        if (conf.isCustomAuth()) {
            byte[] customCred = conf.getCustomCredentials();
            tLen = customCred.length;
            bytesMaxEstimate += tLen;
            initBytes = new byte[bytesMaxEstimate];
            idx = buildConInit(initBytes, idx);  
        
            lenIdx=idx;
            idx = writeHeader(initBytes,idx,0x3);
                        
            idx = littleIndianWriteToArray(initBytes, idx, tLen);
            System.arraycopy(customCred, 0, initBytes, idx, tLen);
            idx+=tLen;
            
        } else {
            CharSequence username = conf.getUserName(this.stageId);
            CharSequence password = conf.getPassword(this.stageId);
            bytesMaxEstimate += ((username.length()+password.length())*6);
            initBytes = new byte[bytesMaxEstimate];
            idx = buildConInit(initBytes, idx);
            
            lenIdx=idx;
            idx = writeHeader(initBytes,idx,0x2);
            
            idx = utf8WriteToArray(username, initBytes, idx);            
            idx = utf8WriteToArray(password, initBytes, idx);                    
        }        
        
        //encode 3 optional missing values for the UUID, ownerUUID and isOwner,  These are encoded as -1 (the only value that would not get confused with something else (TODO: needs to be in developers guide.)
        initBytes[idx++] = -1;
        initBytes[idx++] = -1;
        initBytes[idx++] = -1;
        
        //set this frame length now that we know what the value is.
        int len = idx - 6;//do not count the first 6 connection bytes these are assumed to always be present.
        initBytes[lenIdx++]  = (byte)(0xFF&(len>>0));
        initBytes[lenIdx++]  = (byte)(0xFF&(len>>8));
        initBytes[lenIdx++]  = (byte)(0xFF&(len>>16));
        initBytes[lenIdx++]  = (byte)(0xFF&(len>>24));                                                
                       
        //TODO: we may be able to update this login/connect script later        
        initializerData = ByteBuffer.wrap(initBytes,0,idx);  //only builds connection, connect does not happen till later in run loop.
        initializerData.position(idx); //so we can call flip before each usage.                
        
        pingData = ByteBuffer.allocate(1);
        pingData.put((byte)0xF);
        
        lengthData = ByteBuffer.allocate(4);
        
        //input data can not be any bigger than the output pipe where messages will be sent back to the the caller, we could make this smaller
        inputSocketBuffer = ByteBuffer.allocate(outputMessagesRecieved.sizeOfUntructuredLayoutRingBuffer);
                
    }

    private int buildConInit(byte[] initBytes, int idx) {
        initBytes[idx++] = (byte)'C';
        initBytes[idx++] = (byte)'B';
        initBytes[idx++] = (byte)'2';
        initBytes[idx++] = (byte)'J'; 
        initBytes[idx++] = (byte)'V';
        initBytes[idx++] = (byte)'M';
        return idx;
    }
    
    private int writeInt32(int value, int bytePos, byte[] byteBuffer) {
        byteBuffer[bytePos++] = (byte)(0xFF&(value)); 
        byteBuffer[bytePos++] = (byte)(0xFF&(value>>8));
        byteBuffer[bytePos++] = (byte)(0xFF&(value>>16));
        byteBuffer[bytePos++] = (byte)(0xFF&(value>>24));
        return bytePos;
    }

    private int writeHeader(byte[] initBytes, int idx, int messageType) {
        
        idx+=4; //save room for frame length
        
        initBytes[idx++] = 1;  //version 1 byte const
        initBytes[idx++] = BIT_FLAG_START | BIT_FLAG_END;  //flags   1 byte  begin/end  zeros
        initBytes[idx++] = (byte)(0xFF&messageType);
        initBytes[idx++] = (byte)(0xFF&(messageType>>8));
             
        idx = writeInt32(-1, idx, initBytes);                        
        idx = writeInt32(-1, idx, initBytes);
       
        initBytes[idx++] = 18;  //  2 bytes for data offset, Only NOT 18 when we add data to header in the future. 
        initBytes[idx++] = 0;
        
        return idx;
    }

    private int utf8WriteToArray(CharSequence uuidOwner, byte[] initBytes, int idx) {
        int tLen = Pipe.convertToUTF8(uuidOwner, 0, uuidOwner.length(), initBytes, idx+4, 0xFFFFFFFF);
        idx = littleIndianWriteToArray(initBytes, idx, tLen);
        idx += tLen;
        return idx;
    }

    private int littleIndianWriteToArray(byte[] initBytes, int idx, int tLen) {
        initBytes[idx++] = (byte)(tLen&0xFF); //HZ wants little indian
        initBytes[idx++] = (byte)((tLen>>8)&0xFF);
        initBytes[idx++] = (byte)((tLen>>16)&0xFF);
        initBytes[idx++] = (byte)((tLen>>24)&0xFF);
        return idx;
    }
    
    int exitReason = -1;
    
    @Override
    public void run() {
        //System.err.println(exitReason);
        
        long now = System.currentTimeMillis();
        
        //connect if not connected
        if (!channel.isConnected()) {
            if (!connect(now)) {
                exitReason = 1;
                return;//unable to connect try again later
            }
        }
        
        //if channel is open
        if (channel.isConnected()) {
            
            //this may be the auth credentials so must do now
            if (hasPendingWrites()) {
                if (!nonBlockingByteBufferWrite(now)) {
                    exitReason = 2;
                    return;//do not continue because we have pending writes which must be done first.
                }                   
            }

            if (isAuthenticated) {
                //////////
                //all pending writes have completed so check for need to send the ping
                /////////
                if (timeStampTooOld(now)) {
                    pingData.flip();                
                    pendingWriteBuffers[0] = pingData;
                    
                    if (!nonBlockingByteBufferWrite(now)) {
                        exitReason = 3;
                        return;//try again later, can't send ping now.
                    }
                }                   
    
                //low level read.
                
                while (Pipe.contentToLowLevelRead(inputMessagesToSend, msgSize)) {
                    //is there stuff to send, send it.
                    
                    int msgIdx = Pipe.takeMsgIdx(inputMessagesToSend);                    
                    int meta = Pipe.takeRingByteMetaData(inputMessagesToSend); //for string and byte array
                    int len = Pipe.takeRingByteLen(inputMessagesToSend);
                                        
                    lengthData.clear(); //TODO: once we measure performance if this stage is holding things up we can move this back to the encoder stage.
                    lengthData.put((byte)(0xFF&len));
                    lengthData.put((byte)(0xFF&(len>>8)));
                    lengthData.put((byte)(0xFF&(len>>16)));
                    lengthData.put((byte)(0xFF&(len>>24)));
                    lengthData.flip();                    
                    
                    pendingWriteBuffers[0] = lengthData;
                    pendingWriteBuffers[1] = Pipe.wrappedUnstructuredLayoutBufferA(inputMessagesToSend, meta, len);
                    pendingWriteBuffers[2] = Pipe.wrappedUnstructuredLayoutBufferB(inputMessagesToSend, meta, len);
                    
                    if (!nonBlockingByteBufferWrite(now)) {
                        exitReason = 4;
                        break;
                    }                    
                }   
                
            }
            
            readDataFromConnection();
         
 
            exitReason = 5;
        } else {
            exitReason = 6;
        }
    }

    private void readDataFromConnection() {
        try {
            
            boolean isReenter = inputSocketBuffer.position()>0;
            
            while ( channel.read(inputSocketBuffer) > 0 || isReenter) {
                
                isReenter = false;
                
                //we assume that the data always starts at zero and we are writing at position
                                    
                //first check if its bigger than the smallest frame size then check that we have the full frame
                if (inputSocketBuffer.position()>=18 && 
                    isFrameFullyFilled(inputSocketBuffer) && 
                    Pipe.roomToLowLevelWrite(outputMessagesRecieved, msgSize)) {
                    
                    inputSocketBuffer.flip(); //we are committed to reading the frame at this point
                    ByteBuffer targetBuffer = inputSocketBuffer;
                    
                    int pos = 0;
                    
                    /////////////////////////////////
                    //general checking that what we got is a valid frame
                    ////////////////////////////////
                    int frameSize = readInt32(targetBuffer, pos);
                    int frameStop = pos+frameSize;
                    pos+=4;
                    assert(frameSize>targetBuffer.remaining()) : "Should not have entered this conditional if this were false";                                               
                    
                    int version = targetBuffer.get(pos++);
                    assert(version<2): "No support for other versions";
                    
                    byte masks   = (byte)targetBuffer.get(pos++);
                    assert(0xC0 == masks) : "Should never split an auth response";
                    
                    int taskId = 0xFF&targetBuffer.get(pos++);
                    taskId |= (0xFF&targetBuffer.get(pos++))<<8;
                    
                    pos+=8; //skip correlation and partition - not used by this stage.
                    
                    int offset = 0xFF&targetBuffer.get(pos++);
                    offset |= (0xFF&targetBuffer.get(pos++))<<8;
                    pos+= (offset-18);
                                        
                    switch (taskId) {
                    
                        case 0x6d: //auth response
                            assert(!isAuthenticated);                                
                            
                            authResponse.setLength(0); 
                            
                            //Address
                            int oldPos = pos;
                            pos = readText(targetBuffer, pos, authResponse, frameStop);
                            authAddrLength = pos-oldPos;
                                                            
                            int someNumber = readInt32(targetBuffer, pos);
                            pos+=4;
                            
                            //read Text UUID
                            oldPos = pos;
                            pos = readText(targetBuffer, pos, authResponse, frameStop);
                            authUUIDLength = pos-oldPos;                            
                            
                            //read Text UUIDOwner
                            oldPos = pos;
                            pos = readText(targetBuffer, pos, authResponse, frameStop); //TODO: if the only consumer wants this in UTF8 we may want to store it as is.
                            authUUIDLengthOwner = pos-oldPos; 
                                                            
                            System.err.println("AUTH:"+ authResponse+" "+someNumber);
                            isAuthenticated = true;              
                            
                        break;
                        //Add support for Ping response here
                        
                        
                        
                        
                        
                        default:
                             assert(isAuthenticated);
                             //Send to decode stage                                
                             Pipe.addMsgIdx(outputMessagesRecieved, msgIdx);
                             Pipe.addByteBuffer(targetBuffer, frameSize, outputMessagesRecieved);
                             Pipe.publishWrites(outputMessagesRecieved);
                            
                             Pipe.confirmLowLevelWrite(outputMessagesRecieved, msgSize);
                                                                                 
                    }
                    
                    if (frameStop>=targetBuffer.limit()) {
                       targetBuffer.clear();
                    } else {
                       //copy data down so we have room for the rest                                    
                       int len = targetBuffer.limit()-frameStop;
                       System.arraycopy(targetBuffer.array(), frameStop, targetBuffer.array(), 0, len);
                       targetBuffer.limit(targetBuffer.capacity());
                       targetBuffer.position(len);                                    
                    }
                }
                                    
            }
            
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    static int readInt32(ByteBuffer targetBuffer, int pos) {
        int result = 0xFF&targetBuffer.get(pos);
        result |= (0xFF&targetBuffer.get(pos+1))<<8;
        result |= (0xFF&targetBuffer.get(pos+2))<<16;
        result |= (0xFF&targetBuffer.get(pos+3))<<24;
        return result;
    }
    
    private static boolean isFrameFullyFilled(ByteBuffer buffer) {
                
        int frameSize = 0xFF&buffer.get(0);
        frameSize |= (0xFF&buffer.get(1))<<8;
        frameSize |= (0xFF&buffer.get(2))<<16;
        frameSize |= (0xFF&buffer.get(3))<<24;
        
        return frameSize<=buffer.position();
        
    }

    private static int readText(ByteBuffer targetBuffer, int pos, StringBuilder target, int frameLimit) { //TOOD: Must extract this into a much simpler method.
        
        int length = ConnectionStage.readInt32(targetBuffer, pos);
        int idx = pos+4;
        //May be a text field must try the decode to find out.
        byte[] rawBytes = targetBuffer.array();
        long charAndPos = ((long)idx)<<32;
        long lim = ((long) Math.min(idx+length, frameLimit) )<<32;
        
        while (charAndPos<lim) {
            charAndPos = Pipe.decodeUTF8Fast(rawBytes, charAndPos, 0xFFFFFFFF); 
            char c = (char)charAndPos;
            target.append((char)charAndPos);
        }            
        return idx+length; 

    }
    
    
    private boolean timeStampTooOld(long now) {
        return (now-touched)>timeLimitMS;
    }
    
    private boolean connect(long now) {

        //Note this connection message can also be kicked off because the expected state is connected and the connection was lost.
        //     create socket and connect when we get the connect message        
        try {   
            
            if (!channel.isOpen()) {
                //once a connection is closed it can not be re-opened so we have no choice but create a new connection.
                //NOTE: this is a concern because we now have garbage to be collected.  TODO: X, review what can be done to make this garbage free?
                buildNewChannel();
            }
            
            if (channel.isConnectionPending() || !channel.connect(addr)) {
                if (!channel.finishConnect()) {
                    return false;
                }
            }
            
            if (hasPendingWrites() && pendingWriteBuffers[0] != initializerData) {
                //move them all down.
                int x = pendingWriteBuffers.length-1;
                assert(null==pendingWriteBuffers[x]);
                while (--x >= 1) {
                    pendingWriteBuffers[x] = pendingWriteBuffers[x-1]; //TODO: This is sending too soon? we have not gotten ack back for connect.
                }
            }
            
            initializerData.flip();                     
            pendingWriteBuffers[0] = initializerData;
            System.err.println("XXXXXXXXXXX sending init data of "+initializerData.remaining());
            return nonBlockingByteBufferWrite(now);

        } catch (Throwable t) {
            
            //this is not unreasonable if we are waiting for the broker to be started.
            log.debug("Unable to connect", t);
            buildNewChannel(); //rebuild-connection to start fresh.
            return false;
        }       
    }
    
    private boolean hasPendingWrites() {
        int x = pendingWriteBuffers.length;
        boolean result = false;
        while (--x >= 0) {
            result |= ((null!=pendingWriteBuffers[x])&&(pendingWriteBuffers[x].hasRemaining()));
        }
        return result;
    }
    
    private void buildNewChannel() {
        try {
            channel = (SocketChannel)SocketChannel.open().configureBlocking(false);
            assert(!channel.isBlocking()) : "Blocking must be turned off for all socket connections";   
        } catch (IOException e) {
            throw new RuntimeException("CHECK NETWORK CONNECTION, New non blocking SocketChannel not supported on this platform",e);
        }
    }
    
    private boolean nonBlockingByteBufferWrite(long now) {
        
        int i = 0;
        int limit = pendingWriteBuffers.length;
        while (i<limit) {
            if (null != pendingWriteBuffers[i]) {
                try{
                    if (channel.write(pendingWriteBuffers[i])>0) {
                        touched = now;                  
                    }
                    if (0 == pendingWriteBuffers[i].remaining()) {
                        pendingWriteBuffers[i] = null;
                    } else {
                        //finish later
                        return false;
                    }
                } catch (Exception e) {
                    if (!(e instanceof NotYetConnectedException) && !(e instanceof IOException)) {
                        e.printStackTrace();
                        //TODO: what to do on failure.
                        
                    }
                    return false;
                }
                
            }
            i++;
        }
        return true;
    }

}
