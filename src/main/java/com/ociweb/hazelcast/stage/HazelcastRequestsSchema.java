package com.ociweb.hazelcast.stage;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class HazelcastRequestsSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
        new int[]{0xc0400004,0x80000000,0x80000001,0xa8000000,0xc0200004,0xc0400005,0x80000000,0x80000001,0xa8000000,0xb8000001,0xc0200005},
        (short)0,
        new String[]{"Size","CorrelationID","PartitionHash","Name",null,"Contains","CorrelationID","PartitionHash","Name","Value",null},
        new long[]{1537, 2097136, 2097135, 458497, 0, 1538, 2097136, 2097135, 458497, 458498, 0},
        new String[]{"global",null,null,null,null,"global",null,null,null,null,null},
        "HazelcastSetRequests.xml",
        new long[]{2, 2, 0},
        new int[]{2, 2, 0});
    /* -- full set
        new int[]{0xc0400004,0x80000000,0x80000001,0xa8000000,0xc0200004,0xc0400005,0x80000000,0x80000001,0xa8000000,0xb8000001,0xc0200005,0xc0400005,0x80000000,0x80000001,0xb8000002,0xa8000000,0xc0200005,0xc0400005,0x80000000,0x80000001,0xa8000000,0xb8000001,0xc0200005,0xc0400005,0x80000000,0x80000001,0xa8000000,0xb8000001,0xc0200005,0xc0400005,0x80000000,0x80000001,0xa8000000,0xb8000001,0xc0200005,0xc0400005,0x80000000,0x80000001,0xa8000000,0xb8000002,0xc0200005,0xc0400005,0x80000000,0x80000001,0xa8000000,0xb8000002,0xc0200005,0xc0400004,0x80000000,0x80000001,0xa8000000,0xc0200004,0xc0400004,0x80000000,0x80000001,0xa8000000,0xc0200004,0xc0400005,0x80000000,0x80000001,0xa8000000,0x80400002,0xc0200005,0xc0400005,0x80000000,0x80000001,0xa8000000,0x80400003,0xc0200005,0xc0400004,0x80000000,0x80000001,0xa8000000,0xc0200004},
        (short)0,
        new String[]{"Size","CorrelationID","PartitionHash","Name",null,"Contains","CorrelationID","PartitionHash","Name","Value",null,"ContainsAll","CorrelationID","PartitionHash","ValueSet","Name",null,"Add","CorrelationID","PartitionHash","Name","Value",null,"Remove","CorrelationID","PartitionHash","Name","Value",null,"AddAll","CorrelationID","PartitionHash","Name","ValueList",null,"CompareAndRemoveAll","CorrelationID","PartitionHash","Name","ValueSet",null,"CompareAndRetainAll","CorrelationID","PartitionHash","Name","ValueSet",null,"Clear","CorrelationID","PartitionHash","Name",null,"GetAll","CorrelationID","PartitionHash","Name",null,"AddListener","CorrelationID","PartitionHash","Name","IncludeValue",null,"RemoveListener","CorrelationID","PartitionHash","Name","RegistrationId",null,"IsEmpty","CorrelationID","PartitionHash","Name",null},
        new long[]{1537, 2097136, 2097135, 458497, 0, 1538, 2097136, 2097135, 458497, 458498, 0, 1539, 2097136, 2097135, 458499, 458497, 0, 1540, 2097136, 2097135, 458497, 458498, 0, 1541, 2097136, 2097135, 458497, 458498, 0, 1542, 2097136, 2097135, 458497, 458498, 0, 1543, 2097136, 2097135, 458497, 458499, 0, 1544, 2097136, 2097135, 458497, 458499, 0, 1545, 2097136, 2097135, 458497, 0, 1546, 2097136, 2097135, 458497, 0, 1547, 2097136, 2097135, 458497, 458500, 0, 1548, 2097136, 2097135, 458497, 458501, 0, 1549, 2097136, 2097135, 458497, 0},
        new String[]{"global",null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,"global",null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null},
        "HazelcastSetRequests.xml",
        new long[]{8, 8, 0},
        new int[]{8, 2, 0, 2, 1, 4, 0});
        -- full set */

    public static final HazelcastRequestsSchema instance = new HazelcastRequestsSchema();

    private HazelcastRequestsSchema() {
        super(FROM);
    }

    public static final int MSG_SIZE_1537 = 0x0;
    public static final int MSG_SIZE_1537_FIELD_CORRELATIONID_2097136 = 0x1;
    public static final int MSG_SIZE_1537_FIELD_PARTITIONHASH_2097135 = 0x2;
    public static final int MSG_SIZE_1537_FIELD_NAME_458497 = 0x5000003;
    public static final int MSG_CONTAINS_1538 = 0x5;
    public static final int MSG_CONTAINS_1538_FIELD_CORRELATIONID_2097136 = 0x1;
    public static final int MSG_CONTAINS_1538_FIELD_PARTITIONHASH_2097135 = 0x2;
    public static final int MSG_CONTAINS_1538_FIELD_NAME_458497 = 0x5000003;
    public static final int MSG_CONTAINS_1538_FIELD_VALUE_458498 = 0x7000005;
    public static final int MSG_CONTAINSALL_1539 = 0xb;
    public static final int MSG_CONTAINSALL_1539_FIELD_CORRELATIONID_2097136 = 0x1;
    public static final int MSG_CONTAINSALL_1539_FIELD_PARTITIONHASH_2097135 = 0x2;
    public static final int MSG_CONTAINSALL_1539_FIELD_VALUESET_458499 = 0x7000003;
    public static final int MSG_CONTAINSALL_1539_FIELD_NAME_458497 = 0x5000005;
    public static final int MSG_ADD_1540 = 0x11;
    public static final int MSG_ADD_1540_FIELD_CORRELATIONID_2097136 = 0x1;
    public static final int MSG_ADD_1540_FIELD_PARTITIONHASH_2097135 = 0x2;
    public static final int MSG_ADD_1540_FIELD_NAME_458497 = 0x5000003;
    public static final int MSG_ADD_1540_FIELD_VALUE_458498 = 0x7000005;
    public static final int MSG_REMOVE_1541 = 0x17;
    public static final int MSG_REMOVE_1541_FIELD_CORRELATIONID_2097136 = 0x1;
    public static final int MSG_REMOVE_1541_FIELD_PARTITIONHASH_2097135 = 0x2;
    public static final int MSG_REMOVE_1541_FIELD_NAME_458497 = 0x5000003;
    public static final int MSG_REMOVE_1541_FIELD_VALUE_458498 = 0x7000005;
    public static final int MSG_ADDALL_1542 = 0x1d;
    public static final int MSG_ADDALL_1542_FIELD_CORRELATIONID_2097136 = 0x1;
    public static final int MSG_ADDALL_1542_FIELD_PARTITIONHASH_2097135 = 0x2;
    public static final int MSG_ADDALL_1542_FIELD_NAME_458497 = 0x5000003;
    public static final int MSG_ADDALL_1542_FIELD_VALUELIST_458498 = 0x7000005;
    public static final int MSG_COMPAREANDREMOVEALL_1543 = 0x23;
    public static final int MSG_COMPAREANDREMOVEALL_1543_FIELD_CORRELATIONID_2097136 = 0x1;
    public static final int MSG_COMPAREANDREMOVEALL_1543_FIELD_PARTITIONHASH_2097135 = 0x2;
    public static final int MSG_COMPAREANDREMOVEALL_1543_FIELD_NAME_458497 = 0x5000003;
    public static final int MSG_COMPAREANDREMOVEALL_1543_FIELD_VALUESET_458499 = 0x7000005;
    public static final int MSG_COMPAREANDRETAINALL_1544 = 0x29;
    public static final int MSG_COMPAREANDRETAINALL_1544_FIELD_CORRELATIONID_2097136 = 0x1;
    public static final int MSG_COMPAREANDRETAINALL_1544_FIELD_PARTITIONHASH_2097135 = 0x2;
    public static final int MSG_COMPAREANDRETAINALL_1544_FIELD_NAME_458497 = 0x5000003;
    public static final int MSG_COMPAREANDRETAINALL_1544_FIELD_VALUESET_458499 = 0x7000005;
    public static final int MSG_CLEAR_1545 = 0x2f;
    public static final int MSG_CLEAR_1545_FIELD_CORRELATIONID_2097136 = 0x1;
    public static final int MSG_CLEAR_1545_FIELD_PARTITIONHASH_2097135 = 0x2;
    public static final int MSG_CLEAR_1545_FIELD_NAME_458497 = 0x5000003;
    public static final int MSG_GETALL_1546 = 0x34;
    public static final int MSG_GETALL_1546_FIELD_CORRELATIONID_2097136 = 0x1;
    public static final int MSG_GETALL_1546_FIELD_PARTITIONHASH_2097135 = 0x2;
    public static final int MSG_GETALL_1546_FIELD_NAME_458497 = 0x5000003;
    public static final int MSG_ADDLISTENER_1547 = 0x39;
    public static final int MSG_ADDLISTENER_1547_FIELD_CORRELATIONID_2097136 = 0x1;
    public static final int MSG_ADDLISTENER_1547_FIELD_PARTITIONHASH_2097135 = 0x2;
    public static final int MSG_ADDLISTENER_1547_FIELD_NAME_458497 = 0x5000003;
    public static final int MSG_ADDLISTENER_1547_FIELD_INCLUDEVALUE_458500 = 0x5;
    public static final int MSG_REMOVELISTENER_1548 = 0x3f;
    public static final int MSG_REMOVELISTENER_1548_FIELD_CORRELATIONID_2097136 = 0x1;
    public static final int MSG_REMOVELISTENER_1548_FIELD_PARTITIONHASH_2097135 = 0x2;
    public static final int MSG_REMOVELISTENER_1548_FIELD_NAME_458497 = 0x5000003;
    public static final int MSG_REMOVELISTENER_1548_FIELD_REGISTRATIONID_458501 = 0x5;
    public static final int MSG_ISEMPTY_1549 = 0x45;
    public static final int MSG_ISEMPTY_1549_FIELD_CORRELATIONID_2097136 = 0x1;
    public static final int MSG_ISEMPTY_1549_FIELD_PARTITIONHASH_2097135 = 0x2;
    public static final int MSG_ISEMPTY_1549_FIELD_NAME_458497 = 0x5000003;
}
