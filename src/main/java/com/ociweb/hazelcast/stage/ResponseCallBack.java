package com.ociweb.hazelcast.stage;

import com.ociweb.pronghorn.pipe.LittleEndianDataInputBlobReader;

public interface ResponseCallBack {

    void send(int correlationId, int type, int partitionId, LittleEndianDataInputBlobReader<RequestResponseSchema> reader);

}
