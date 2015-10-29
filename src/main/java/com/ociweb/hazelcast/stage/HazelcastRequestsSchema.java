package com.ociweb.hazelcast.stage;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class HazelcastRequestsSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager hzRequests = new FieldReferenceOffsetManager(
        new int[]{0xc0400004,0x80000000,0x80000001,0xa0000000,0xc0200004},
        (short)0,
        new String[]{"Size","CorrelationID","PartitionHash","Name",null},
        new long[]{1537, 2097136, 2097135, 458497, 0},
        new String[]{"global",null,null,null,null},
        "HazelcastSetRequests.xml");

    public static final HazelcastRequestsSchema instance = new HazelcastRequestsSchema();

    private HazelcastRequestsSchema() {
        super(hzRequests);
    }
}
