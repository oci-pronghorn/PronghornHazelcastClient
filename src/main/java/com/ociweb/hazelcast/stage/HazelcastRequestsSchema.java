package com.ociweb.hazelcast.stage;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class HazelcastRequestsSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
        new int[]{0xc0400005,0x80000000,0x80000001,0xa8000000,0xb8000001,0xc0200005},
        (short)0,
        new String[]{"Add","CorrelationID","PartitionHash","Name","Value",null},
        new long[]{1540, 2097136, 2097135, 458497, 458498, 0},
        new String[]{"global",null,null,null,null,null},
        "HazelcastSetRequests.xml",
        new long[]{2, 2, 0},
        new int[]{2, 2, 0});

    public static final HazelcastRequestsSchema instance = new HazelcastRequestsSchema();

    private HazelcastRequestsSchema() {
        super(FROM);
    }

    public static final int MSG_ADD_1540 = 0x00000000;
    public static final int MSG_ADD_1540_FIELD_CORRELATIONID_2097136 = 0x00000001;
    public static final int MSG_ADD_1540_FIELD_PARTITIONHASH_2097135 = 0x00000002;
    public static final int MSG_ADD_1540_FIELD_NAME_458497 = 0x01400003;
    public static final int MSG_ADD_1540_FIELD_VALUE_458498 = 0x01C00005;
}
