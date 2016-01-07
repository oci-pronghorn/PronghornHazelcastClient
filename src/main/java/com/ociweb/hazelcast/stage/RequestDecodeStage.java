package com.ociweb.hazelcast.stage;

import java.io.IOException;

import com.ociweb.hazelcast.HZDataInput;
import com.ociweb.pronghorn.pipe.LittleEndianDataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * This class reads a response from the Hazelcast cluster in RawDataFormat. It decodes the response into a Correlation
 * ID and a HazelcastResponse and invokes the Callback associated with the Correlation ID.
 * <p/>
 * The fragmented messages are re-assembed in this stage. As a result, the begin and end flags are always set regardless
 * of what was received from the server.
 */

public class RequestDecodeStage extends PronghornStage {

    private Pipe<RequestResponseSchema>[] inputFromConnection;
    private HZDataInput[] readers;
    private static final int msgSize = RequestResponseSchema.FROM.fragDataSize[RequestResponseSchema.MSG_RESPONSE_1];

    private static final int BEGIN_FLAG = 128;
    private static final int END_FLAG = 64;


    private final ResponseCallBack callBack;

    public RequestDecodeStage(GraphManager gm, Pipe<RequestResponseSchema>[] inputFromConnection, HazelcastConfigurator configurator) {
        super(gm, inputFromConnection, NONE);
        this.inputFromConnection = inputFromConnection;

        // Get from configurator?
        this.callBack = new ResponseCallBack() {
            @Override
            public void send(int correlationId, short type, short flags, int partitionId, HZDataInput reader) {
                try {
                    System.out.println("data from correlationId " + correlationId + " with bytes " + reader.available());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

    }

    public RequestDecodeStage(GraphManager gm, Pipe<RequestResponseSchema>[] inputFromConnection, ResponseCallBack callBack) {
        super(gm, inputFromConnection, NONE);
        this.inputFromConnection = inputFromConnection;
        this.callBack = callBack;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void startup() {
        int j = inputFromConnection.length;
        readers = new HZDataInput[j];
        while (--j >= 0) {
            readers[j] = new HZDataInput(inputFromConnection[j]);
        }
    }

    @Override
    public void run() {
        int c;
        do {
            c = 0;
            int j = inputFromConnection.length;
            while (--j >= 0) {
                c += readFromPipe(inputFromConnection[j], readers[j]);
            }
        } while (c > 0);//keep going until we find that no pipes have any data

    }

    private int readFromPipe(Pipe<RequestResponseSchema> pipe, HZDataInput reader) {
        int c = 0;
        while (Pipe.hasContentToRead(pipe)) { //keep going while this pipe has data

            int msgIdx = Pipe.takeMsgIdx(pipe);
            assert (RequestResponseSchema.MSG_RESPONSE_1 == msgIdx) : "Only one message template is supported";

            int typeFlags = Pipe.takeValue(pipe);
            int correlationId = Pipe.takeValue(pipe);
            int partitionId = Pipe.takeValue(pipe);

            if (0 != (BEGIN_FLAG & typeFlags)) {
                reader.openLowLevelAPIField();
            } else {
                //combine this new field with the bytes so far
                reader.accumLowLevelAPIField();
            }
            Pipe.confirmLowLevelRead(pipe, msgSize);
            Pipe.readNextWithoutReleasingReadLock(pipe);

            if (0 != (END_FLAG & typeFlags)) {
                callBack.send(correlationId, (short) (typeFlags >> 16), (short) ((BEGIN_FLAG | END_FLAG) | (0x3F & typeFlags)), partitionId, reader);
                Pipe.releaseAllPendingReadLock(pipe);

            }
        }

        return c;
    }


}
