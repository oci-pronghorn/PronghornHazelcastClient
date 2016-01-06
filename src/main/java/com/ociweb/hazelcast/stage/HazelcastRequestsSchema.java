package com.ociweb.hazelcast.stage;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class HazelcastRequestsSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
        new int[]{0xc0400005,0x80000000,0x80000001,0xa8000000,0xa8000001,0xc0200005,0xc0400005,0x80000000,0x80000001,0xa8000000,0xa8000001,0xc0200005,0xc0400003,0x80000000,0x80000001,0xc0200003,0xc0400004,0x80000000,0x80000001,0xa8000002,0xc0200004,0xc0400005,0x80000000,0x80000001,0xa8000002,0xb8000003,0xc0200005,0xc0400005,0x80000000,0x80000001,0xa8000002,0xb8000004,0xc0200005,0xc0400005,0x80000000,0x80000001,0xa8000002,0xb8000003,0xc0200005},
        (short)0,
        new String[]{"CreateProxy","CorrelationID","PartitionHash","Name","ServiceName",null,"DestroyProxy","CorrelationID","PartitionHash","Name","ServiceName",null,"GetPartitions","CorrelationID","PartitionHash",null,"Size","CorrelationID","PartitionHash","Name",null,"Contains","CorrelationID","PartitionHash","Name","Value",null,"ContainsAll","CorrelationID","PartitionHash","Name","ValueSet",null,"Add","CorrelationID","PartitionHash","Name","Value",null},
        new long[]{5, 2097136, 2097135, 2096903, 2096904, 0, 6, 2097136, 2097135, 2096903, 2096904, 0, 8, 2097136, 2097135, 0, 1537, 2097136, 2097135, 458497, 0, 1538, 2097136, 2097135, 458497, 458498, 0, 1539, 2097136, 2097135, 458497, 458499, 0, 1540, 2097136, 2097135, 458497, 458498, 0},
        new String[]{"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,"global",null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null},
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
    public static final int MSG_DESTROYPROXY_6 = 0x00000006;
    public static final int MSG_DESTROYPROXY_6_FIELD_CORRELATIONID_2097136 = 0x00000001;
    public static final int MSG_DESTROYPROXY_6_FIELD_PARTITIONHASH_2097135 = 0x00000002;
    public static final int MSG_DESTROYPROXY_6_FIELD_NAME_2096903 = 0x01400003;
    public static final int MSG_DESTROYPROXY_6_FIELD_SERVICENAME_2096904 = 0x01400005;
    public static final int MSG_GETPARTITIONS_8 = 0x0000000C;
    public static final int MSG_GETPARTITIONS_8_FIELD_CORRELATIONID_2097136 = 0x00000001;
    public static final int MSG_GETPARTITIONS_8_FIELD_PARTITIONHASH_2097135 = 0x00000002;
    public static final int MSG_SIZE_1537 = 0x00000010;
    public static final int MSG_SIZE_1537_FIELD_CORRELATIONID_2097136 = 0x00000001;
    public static final int MSG_SIZE_1537_FIELD_PARTITIONHASH_2097135 = 0x00000002;
    public static final int MSG_SIZE_1537_FIELD_NAME_458497 = 0x01400003;
    public static final int MSG_CONTAINS_1538 = 0x00000015;
    public static final int MSG_CONTAINS_1538_FIELD_CORRELATIONID_2097136 = 0x00000001;
    public static final int MSG_CONTAINS_1538_FIELD_PARTITIONHASH_2097135 = 0x00000002;
    public static final int MSG_CONTAINS_1538_FIELD_NAME_458497 = 0x01400003;
    public static final int MSG_CONTAINS_1538_FIELD_VALUE_458498 = 0x01C00005;
    public static final int MSG_CONTAINSALL_1539 = 0x0000001B;
    public static final int MSG_CONTAINSALL_1539_FIELD_CORRELATIONID_2097136 = 0x00000001;
    public static final int MSG_CONTAINSALL_1539_FIELD_PARTITIONHASH_2097135 = 0x00000002;
    public static final int MSG_CONTAINSALL_1539_FIELD_NAME_458497 = 0x01400003;
    public static final int MSG_CONTAINSALL_1539_FIELD_VALUESET_458499 = 0x01C00005;
    public static final int MSG_ADD_1540 = 0x00000021;
    public static final int MSG_ADD_1540_FIELD_CORRELATIONID_2097136 = 0x00000001;
    public static final int MSG_ADD_1540_FIELD_PARTITIONHASH_2097135 = 0x00000002;
    public static final int MSG_ADD_1540_FIELD_NAME_458497 = 0x01400003;
    public static final int MSG_ADD_1540_FIELD_VALUE_458498 = 0x01C00005;
}

