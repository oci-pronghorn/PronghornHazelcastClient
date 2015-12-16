package com.ociweb.hazelcast.stage;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class HazelcastRequestsSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
        new int[]{0xc0400005,0x80000000,0x80000001,0xa8000000,0xa8000001,0xc0200005,0xc0400004,0x80000000,0x80000001,0xa8000002,0xc0200004},
        (short)0,
        new String[]{"CreateProxy","CorrelationID","PartitionHash","Name","ServiceName",null,"Size","CorrelationID","PartitionHash","Name",null},
        new long[]{5, 2097136, 2097135, 2096903, 2096904, 0, 1537, 2097136, 2097135, 458497, 0},
        new String[]{"global",null,null,null,null,null,"global",null,null,null,null},
        "HazelcastSetRequests.xml",
        new long[]{2, 2, 0},
        new int[]{2, 2, 0});

    public static final HazelcastRequestsSchema instance = new HazelcastRequestsSchema();

    private HazelcastRequestsSchema() {
        super(FROM);
    }
    public static final int MSG_CREATEPROXY_5 = 0x00000000;
    public static final int MSG_CREATEPROXY_5_FIELD_CORRELATIONID_2097136 = 0x00000001;
    public static final int MSG_CREATEPROXY_5_FIELD_PARTITIONHASH_2097135 = 0x00000002;
    public static final int MSG_CREATEPROXY_5_FIELD_NAME_2096903 = 0x01400003;
    public static final int MSG_CREATEPROXY_5_FIELD_SERVICENAME_2096904 = 0x01400005;
    public static final int MSG_SIZE_1537 = 0x00000006;
    public static final int MSG_SIZE_1537_FIELD_CORRELATIONID_2097136 = 0x00000001;
    public static final int MSG_SIZE_1537_FIELD_PARTITIONHASH_2097135 = 0x00000002;
    public static final int MSG_SIZE_1537_FIELD_NAME_458497 = 0x01400003;

}
