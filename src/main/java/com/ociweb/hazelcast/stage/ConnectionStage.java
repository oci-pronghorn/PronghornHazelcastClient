package com.ociweb.hazelcast.stage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import com.ociweb.hazelcast.HazelcastConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.LittleEndianDataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ConnectionStage extends PronghornStage {

    private final static Logger log = LoggerFactory.getLogger(ConnectionStage.class);

    private final Pipe<RawDataSchema> inputMessagesToSend;

    private final Pipe<RequestResponseSchema>[] outputMessagesReceived;
    private final long[] outputActiveCorrelationId;
    private static final int END_FLAG = 64;

    private final HazelcastConfigurator conf;

    private final long pingTimeSeconds = 200;
    private final long timeLimitMilliSeconds = pingTimeSeconds * 1000;//TODO: where to set this ping?

    protected boolean isAuthenticated = false;
    private long touched;

    private final static byte BIT_FLAG_START = (byte) 0x80;
    private final static byte BIT_FLAG_END = (byte) 0x40;

    private InetSocketAddress addr;
    private SocketChannel channel;
    private ByteBuffer initializerData;
    private ByteBuffer pingData;
    private ByteBuffer lengthData;

    private ByteBuffer[] pendingWriteBuffers;

    protected int authPort = -1;
    protected int authAddrLen = -1;
    protected int authUUIDLen = -1;
    protected int authUUIDOwner = -1;
    protected StringBuilder authResponse = new StringBuilder(128);

    //NOTE: in the future if this is a performance issue we can extract a routing stage out of this connectionStages
    private PipeConfig<RawDataSchema> inputSocketPipeConfig;
    private Pipe<RawDataSchema> inputSocketPipe;
    private LittleEndianDataInputBlobReader<RawDataSchema> reader;

    @SuppressWarnings("unchecked")
    public ConnectionStage(GraphManager graphManager,
                           Pipe<RawDataSchema> input,
                           Pipe<RequestResponseSchema> output,
                           HazelcastConfigurator conf) {
        super(graphManager, input, output);
        this.inputMessagesToSend = input;
        this.outputMessagesReceived = new Pipe[]{output};

        this.outputActiveCorrelationId = new long[1];
        Arrays.fill(outputActiveCorrelationId, Long.MIN_VALUE);

        this.conf = conf;
    }

    protected ConnectionStage(GraphManager graphManager,
                              Pipe<RawDataSchema> input,
                              Pipe<RequestResponseSchema>[] outputs,
                              HazelcastConfigurator conf) {
        super(graphManager, input, outputs);
        this.inputMessagesToSend = input;
        this.outputMessagesReceived = outputs;

        this.outputActiveCorrelationId = new long[outputs.length];
        Arrays.fill(outputActiveCorrelationId, Long.MIN_VALUE);

        this.conf = conf;
    }

    @Override
    public void startup() {
        addr = conf.buildInetSocketAddress(this.stageId); //may also get called later if this node should go down.
        buildNewChannel();

        pendingWriteBuffers = new ByteBuffer[3];

        CharSequence uuid = conf.getUUID(this.stageId);
        CharSequence uuidOwner = conf.getOwnerUUID(this.stageId);

        int bytesMaxEstimate = 100 + 3 + 18 + 3 + ((uuid.length() + uuidOwner.length()) * 6);

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

            lenIdx = idx;
            idx = writeHeader(initBytes, idx, -1, -1, 0x3);

            idx = littleIndianWriteToArray(initBytes, idx, tLen);
            System.arraycopy(customCred, 0, initBytes, idx, tLen);
            idx += tLen;

        } else {
            CharSequence username = conf.getUserName(this.stageId);
            CharSequence password = conf.getPassword(this.stageId);
            bytesMaxEstimate += ((username.length() + password.length()) * 6);
            initBytes = new byte[bytesMaxEstimate];
            idx = buildConInit(initBytes, idx);

            lenIdx = idx;
            idx = writeHeader(initBytes, idx, -1, -1, 0x2);

            idx = utf8WriteToArray(username, initBytes, idx);
            idx = utf8WriteToArray(password, initBytes, idx);
        }

        //encode 3 optional missing values for the UUID, ownerUUID and isOwner,  These are encoded as -1 (the only value that would not get confused with something else (TODO: needs to be in developers guide.)
        initBytes[idx++] = -1;
        initBytes[idx++] = -1;
        initBytes[idx++] = -1;

        //set this frame length now that we know what the value is.
        int len = idx - 6;//do not count the first 6 connection bytes these are assumed to always be present.
        initBytes[lenIdx++] = (byte) (0xFF & (len));
        initBytes[lenIdx++] = (byte) (0xFF & (len >> 8));
        initBytes[lenIdx++] = (byte) (0xFF & (len >> 16));
        initBytes[lenIdx++] = (byte) (0xFF & (len >> 24));

        //TODO: we may be able to update this login/connect script later
        initializerData = ByteBuffer.wrap(initBytes, 0, idx);  //only builds connection, connect does not happen till later in run loop.
        initializerData.position(idx); //so we can call flip before each usage.

        pingData = ByteBuffer.allocate(1);
        pingData.put((byte) 0xF);

        lengthData = ByteBuffer.allocate(4);

        allocateIncomingBuffer();
    }


    private void allocateIncomingBuffer() {
        int size = Integer.MAX_VALUE;
        int j = outputMessagesReceived.length;
        if (j == 0) {
            throw new UnsupportedOperationException("Must have at least 1 output pipe.");
        }
        while (--j >= 0) {
            size = Math.min(size, outputMessagesReceived[j].sizeOfBlobRing);
        }

        inputSocketPipeConfig = new PipeConfig<RawDataSchema>(RawDataSchema.instance, 20, size);
        inputSocketPipe = new Pipe<RawDataSchema>(inputSocketPipeConfig);
        inputSocketPipe.initBuffers();
        reader = new LittleEndianDataInputBlobReader<RawDataSchema>(inputSocketPipe);
        LittleEndianDataInputBlobReader.openRawRead(reader, 0, 0);//must be zero length to start
    }

    private int buildConInit(byte[] initBytes, int idx) {
        initBytes[idx++] = (byte) 'C';
        initBytes[idx++] = (byte) 'B';
        initBytes[idx++] = (byte) '2';
        initBytes[idx++] = (byte) 'J';
        initBytes[idx++] = (byte) 'V';
        initBytes[idx++] = (byte) 'M';
        return idx;
    }

    private static int writeInt32(int value, int bytePos, byte[] byteBuffer) {
        byteBuffer[bytePos++] = (byte) (0xFF & (value));
        byteBuffer[bytePos++] = (byte) (0xFF & (value >> 8));
        byteBuffer[bytePos++] = (byte) (0xFF & (value >> 16));
        byteBuffer[bytePos++] = (byte) (0xFF & (value >> 24));
        return bytePos;
    }

    static int writeHeader(byte[] initBytes, int idx, int corId, int parId, int messageType) {

        idx += 4; //save room for frame length

        initBytes[idx++] = 1;  //version 1 byte const
        initBytes[idx++] = BIT_FLAG_START | BIT_FLAG_END;  //flags   1 byte  begin/end  zeros
        initBytes[idx++] = (byte) (0xFF & messageType);
        initBytes[idx++] = (byte) (0xFF & (messageType >> 8));

        idx = writeInt32(corId, idx, initBytes);
        idx = writeInt32(parId, idx, initBytes);

        initBytes[idx++] = 18;  //  2 bytes for data offset, Only NOT 18 when we add data to header in the future.
        initBytes[idx++] = 0;

        return idx;
    }

    private int utf8WriteToArray(CharSequence uuidOwner, byte[] initBytes,
                                 int idx) {
        int tLen = Pipe.convertToUTF8(uuidOwner, 0, uuidOwner.length(), initBytes, idx + 4, 0xFFFFFFFF);
        idx = littleIndianWriteToArray(initBytes, idx, tLen);
        idx += tLen;
        return idx;
    }

    private int littleIndianWriteToArray(byte[] initBytes, int idx, int tLen) {
        initBytes[idx++] = (byte) (tLen & 0xFF); //HZ wants little indian
        initBytes[idx++] = (byte) ((tLen >> 8) & 0xFF);
        initBytes[idx++] = (byte) ((tLen >> 16) & 0xFF);
        initBytes[idx++] = (byte) ((tLen >> 24) & 0xFF);
        return idx;
    }

    int exitReason = -1;

    @Override
    public void run() {
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
                System.out.println("hasPendingWrites calling buffer write");
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

                    System.out.println("timeStampTooOld calling buffer write");
                    if (!nonBlockingByteBufferWrite(now)) {
                        exitReason = 3;
                        return;//try again later, can't send ping now.
                    }
                }

                // low level read.
                while (Pipe.hasContentToRead(inputMessagesToSend)) {
                    //is there stuff to send, send it.

                    int msgIdx = Pipe.takeMsgIdx(inputMessagesToSend);
                    int meta = Pipe.takeRingByteMetaData(inputMessagesToSend); //for string and byte array
                    int len = Pipe.takeRingByteLen(inputMessagesToSend);
/*                    if (len < 18) {
                        throw new UnsupportedOperationException("Error there are no messages smaller than the 18 byte header and we got "+len);
                    }
                    pendingWriteBuffers[0] = Pipe.wrappedBlobReadingRingA(inputMessagesToSend, meta, len);

                    pendingWriteBuffers[1] = Pipe.wrappedBlobReadingRingB(inputMessagesToSend, meta, len);
                    pendingWriteBuffers[2] = null;
 */
                    int vli = len + 4;
                    lengthData.clear(); //TODO: once we measure performance if this stage is holding things up we can move this back to the encoder stage.
                    lengthData.put((byte) (0xFF & vli));
                    lengthData.put((byte) (0xFF & (vli >> 8)));
                    lengthData.put((byte) (0xFF & (vli >> 16)));
                    lengthData.put((byte) (0xFF & (vli >> 24)));
                    lengthData.flip();

                    pendingWriteBuffers[0] = lengthData;
                    pendingWriteBuffers[1] = Pipe.wrappedBlobReadingRingA(inputMessagesToSend, meta, len);
                    pendingWriteBuffers[2] = Pipe.wrappedBlobReadingRingB(inputMessagesToSend, meta, len);

                    if (!nonBlockingByteBufferWrite(now)) {
                        exitReason = 4;
                        break;
                    }

                    Pipe.confirmLowLevelRead(inputMessagesToSend, Pipe.sizeOf(inputMessagesToSend, msgIdx));
                    Pipe.releaseReadLock(inputMessagesToSend);
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
            //continues to run while there is room and the channel has data
            pumpByteChannelIntoRawDataPipe(channel, inputSocketPipe); //Move as part of the new WebServer

            //accum data on to the reader stream, enables the crossing of message boundaries.
            LittleEndianDataInputBlobReader.appendNextFieldToReader(reader, inputSocketPipe);

            //now consume the frames off the ring in blocks larger than how we wrote it.
            consumeFrameIfPresent(inputSocketPipe, reader);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void consumeFrameIfPresent(Pipe<RawDataSchema> targetPipe,
            LittleEndianDataInputBlobReader<RawDataSchema> localReader) throws IOException {
        int available = localReader.available();
        int frameByteCount;
        if (available>=18 && (frameByteCount = localReader.peekInt()) <= available) {
            int pipeIdx = selectOutputPipe();
            if (pipeIdx < 0) {
                return;//do nothing, can not find pipe
            }

            consumeFrameFromReader(pipeIdx);
            Pipe.releasePendingAsReadLock(targetPipe, frameByteCount);
        }
    }

    public void pumpByteChannelIntoRawDataPipe(ReadableByteChannel sourceChannel, Pipe<RawDataSchema> targetPipe) throws IOException {
        while (Pipe.hasRoomForWrite(targetPipe)) {

            int originalBlobPosition = Pipe.bytesWorkingHeadPosition(targetPipe);
            ByteBuffer targetByteBuffer = Pipe.wrappedBlobForWriting(originalBlobPosition, targetPipe);
            int len;//if data is read then we build a record around it
            if ((len = sourceChannel.read(targetByteBuffer))>0) {
                int size = Pipe.addMsgIdx(targetPipe,RawDataSchema.MSG_CHUNKEDSTREAM_1);
                Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, len, targetPipe);

                Pipe.confirmLowLevelWrite(targetPipe, size);
                Pipe.publishWrites(targetPipe);
            } else {
                //if nothing was read then the channel is empty, try again later.
                break;
            }
        }
    }

    public void consumeFrameFromReader(int pipeIdx) throws IOException {
        Pipe<RequestResponseSchema> selectedPipe = outputMessagesReceived[pipeIdx];

        //first check if its bigger than the smallest frame size then check that we have the full frame
        if (Pipe.hasRoomForWrite(selectedPipe)) {
            int frameSize = reader.readInt();
            int version = reader.readByte();
            assert (version < 2) : "No support for other versions";
            int flags = reader.readByte();
            int type = reader.readShort();//taskId;
            int correlationId = reader.readInt();
            int partitionId = reader.readInt();
            int offset = reader.readShort();

            reader.skip(offset - 18);
            int remainingBytes = frameSize - offset;

            switch (type) {

                case 0x6d: //auth response
                    assert (!isAuthenticated);

                    authResponse.setLength(0);

                    //address
                    authAddrLen = LittleEndianDataInputBlobReader.readUTF(reader, authResponse, reader.readInt());

                    authPort = reader.readInt();

                    // text UUID
                    authUUIDLen = LittleEndianDataInputBlobReader.readUTF(reader, authResponse, reader.readInt());

                    // text UUID owner
                    authUUIDOwner = LittleEndianDataInputBlobReader.readUTF(reader, authResponse, reader.readInt());

                    isAuthenticated = true;

                    break;

                // TODO: Add support for Ping response here

                default:
                    assert (isAuthenticated);

                    //TODO: write to free pipe, if none found then fail need more pipes.
                    int responseSize = Pipe.addMsgIdx(selectedPipe, RequestResponseSchema.MSG_RESPONSE_1);

                    //send type in the upper 16 and the flags in the lower
                    Pipe.addIntValue((type << 16) | flags, selectedPipe);
                    Pipe.addIntValue(correlationId, selectedPipe);
                    Pipe.addIntValue(partitionId, selectedPipe);

                    reader.readInto(selectedPipe, remainingBytes);

                    Pipe.confirmLowLevelWrite(selectedPipe, responseSize);
                    Pipe.publishWrites(selectedPipe);

                    if (0 != (flags & END_FLAG)) {
                        outputActiveCorrelationId[pipeIdx] = Long.MIN_VALUE;
                    } else {
                        //this is only a fragment so block this pipe until we get the end.
                        outputActiveCorrelationId[pipeIdx] = correlationId;
                    }

            }

        }
    }

    private int selectOutputPipe() {
        final int correlationPeek = LittleEndianDataInputBlobReader.peekInt(reader, 8);

        int selectedPipe = -1;
        //find a free pipe
        int j = outputActiveCorrelationId.length;
        while (--j >= 0) {
            //only selected if one has not been selected
            if (selectedPipe < 0 && Long.MIN_VALUE == outputActiveCorrelationId[j] && Pipe.hasRoomForWrite(outputMessagesReceived[j])) {
                selectedPipe = j;
            }
            if (correlationPeek == outputActiveCorrelationId[j]) {
                return j;
            }
        }
        return selectedPipe;
    }

    private static boolean isFrameFullyFilled(ByteBuffer buffer) {

        int frameSize = 0xFF & buffer.get(0);
        frameSize |= (0xFF & buffer.get(1)) << 8;
        frameSize |= (0xFF & buffer.get(2)) << 16;
        frameSize |= (0xFF & buffer.get(3)) << 24;

        return frameSize <= buffer.position();

    }

    private boolean timeStampTooOld(long now) {
        return (now - touched) > timeLimitMilliSeconds;
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
                int x = pendingWriteBuffers.length - 1;
                assert (null == pendingWriteBuffers[x]);
                while (--x >= 1) {
                    pendingWriteBuffers[x] = pendingWriteBuffers[x - 1]; //TODO: This is sending too soon? we have not gotten ack back for connect.
                }
            }

            initializerData.flip();
            pendingWriteBuffers[0] = initializerData;
            log.debug("Sending initialization data of " + initializerData.remaining());
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
            result |= ((null != pendingWriteBuffers[x]) && (pendingWriteBuffers[x].hasRemaining()));
        }
        return result;
    }

    private void buildNewChannel() {
        try {
            channel = (SocketChannel) SocketChannel.open().configureBlocking(false);
            assert (!channel.isBlocking()) : "Blocking must be turned off for all socket connections";
        } catch (IOException e) {
            throw new RuntimeException("CHECK NETWORK CONNECTION, New non blocking SocketChannel not supported on this platform", e);
        }
    }

    private boolean nonBlockingByteBufferWrite(long now) {
        for (int i = 0; i < pendingWriteBuffers.length; i++) {
            if (null != pendingWriteBuffers[i] && pendingWriteBuffers[i].hasRemaining()) {
                try {
                    if (channel.write(pendingWriteBuffers[i]) > 0) {
                        touched = now;
                    }
                    if (0 == pendingWriteBuffers[i].remaining()) {
                        pendingWriteBuffers[i] = null;
                    } else {
                        System.out.println("ConnectionStage.nonBlockingByteBufferWrite: finish later with "+ pendingWriteBuffers[i].remaining());
                        //finish later
                        return false;
                    }
                } catch (Exception e) {
                    if (!(e instanceof NotYetConnectedException) && !(e instanceof IOException)) {
                        e.printStackTrace();
                        //TODO: what to do on failure.
                    }
                    System.out.println("ConnectionStage.nonBlockingByteBufferWrite: Error and unable to send");
                    return false;
                }
            }
        }
        return true;
    }

}
