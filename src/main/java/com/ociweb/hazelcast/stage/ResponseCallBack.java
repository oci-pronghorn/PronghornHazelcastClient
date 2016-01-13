package com.ociweb.hazelcast.stage;

import com.ociweb.hazelcast.HZDataInput;

public interface ResponseCallBack {
    void send(int correlationId, short type, short flags, int partitionId, HZDataInput reader);
}
