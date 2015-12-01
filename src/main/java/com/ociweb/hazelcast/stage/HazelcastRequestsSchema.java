package com.ociweb.hazelcast.stage;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class HazelcastRequestsSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
        new int[]{0xc0400004,0x80000000,0x80000001,0xa8000000,0xc0200004},
        (short)0,
        new String[]{"Size","CorrelationID","PartitionHash","Name",null},
        new long[]{1537, 2097136, 2097135, 458497, 0},
        new String[]{"global",null,null,null,null},
        "HazelcastSetRequests.xml",
        new long[]{2, 2, 0},
        new int[]{2, 2, 0});
    /* -- first three
        new int[]{0xc0400004,0x80000000,0x80000001,0xa8000000,0xc0200004,0xc0400005,0x80000000,0x80000001,0xa8000000,0xb8000001,0xc0200005,0xc0400005,0x80000000,0x80000001,0xa8000000,0xb8000002,0xc0200005},
        (short)0,
        new String[]{"Size","CorrelationID","PartitionHash","Name",null,"Contains","CorrelationID","PartitionHash","Name","Value",null,"ContainsAll","CorrelationID","PartitionHash","Name","ValueSet",null},
        new long[]{1537, 2097136, 2097135, 458497, 0, 1538, 2097136, 2097135, 458497, 458498, 0, 1539, 2097136, 2097135, 458497, 458499, 0},
        new String[]{"global",null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null},
        "HazelcastSetRequests.xml",
        new long[]{2, 2, 0},
        new int[]{2, 2, 0});
    */
    /* -- full set
        new int[]{0xc0400004,0x80000000,0x80000001,0xa8000000,0xc0200004,0xc0400005,0x80000000,0x80000001,0xa8000000,0xb8000001,0xc0200005,0xc0400005,0x80000000,0x80000001,0xb8000002,0xa8000000,0xc0200005,0xc0400005,0x80000000,0x80000001,0xa8000000,0xb8000001,0xc0200005,0xc0400005,0x80000000,0x80000001,0xa8000000,0xb8000001,0xc0200005,0xc0400005,0x80000000,0x80000001,0xa8000000,0xb8000001,0xc0200005,0xc0400005,0x80000000,0x80000001,0xa8000000,0xb8000002,0xc0200005,0xc0400005,0x80000000,0x80000001,0xa8000000,0xb8000002,0xc0200005,0xc0400004,0x80000000,0x80000001,0xa8000000,0xc0200004,0xc0400004,0x80000000,0x80000001,0xa8000000,0xc0200004,0xc0400005,0x80000000,0x80000001,0xa8000000,0x80400002,0xc0200005,0xc0400005,0x80000000,0x80000001,0xa8000000,0x80400003,0xc0200005,0xc0400004,0x80000000,0x80000001,0xa8000000,0xc0200004},
        (short)0,
        new String[]{"Size","CorrelationID","PartitionHash","Name",null,"Contains","CorrelationID","PartitionHash","Name","Value",null,"ContainsAll","CorrelationID","PartitionHash","ValueSet","Name",null,"Add","CorrelationID","PartitionHash","Name","Value",null,"Remove","CorrelationID","PartitionHash","Name","Value",null,"AddAll","CorrelationID","PartitionHash","Name","ValueList",null,"CompareAndRemoveAll","CorrelationID","PartitionHash","Name","ValueSet",null,"CompareAndRetainAll","CorrelationID","PartitionHash","Name","ValueSet",null,"Clear","CorrelationID","PartitionHash","Name",null,"GetAll","CorrelationID","PartitionHash","Name",null,"AddListener","CorrelationID","PartitionHash","Name","IncludeValue",null,"RemoveListener","CorrelationID","PartitionHash","Name","RegistrationId",null,"IsEmpty","CorrelationID","PartitionHash","Name",null},
        new long[]{1537, 2097136, 2097135, 458497, 0, 1538, 2097136, 2097135, 458497, 458498, 0, 1539, 2097136, 2097135, 458499, 458497, 0, 1540, 2097136, 2097135, 458497, 458498, 0, 1541, 2097136, 2097135, 458497, 458498, 0, 1542, 2097136, 2097135, 458497, 458498, 0, 1543, 2097136, 2097135, 458497, 458499, 0, 1544, 2097136, 2097135, 458497, 458499, 0, 1545, 2097136, 2097135, 458497, 0, 1546, 2097136, 2097135, 458497, 0, 1547, 2097136, 2097135, 458497, 458500, 0, 1548, 2097136, 2097135, 458497, 458501, 0, 1549, 2097136, 2097135, 458497, 0},
        new String[]{"global",null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,"global",null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null},
        -- full set */

    public static final HazelcastRequestsSchema instance = new HazelcastRequestsSchema();

    private HazelcastRequestsSchema() {
        super(FROM);
    }

    public static final int MSG_SIZE_1537 = 0x00000000;
    public static final int MSG_SIZE_1537_FIELD_CORRELATIONID_2097136 = 0x00000001;
    public static final int MSG_SIZE_1537_FIELD_PARTITIONHASH_2097135 = 0x00000002;
    public static final int MSG_SIZE_1537_FIELD_NAME_458497 = 0x01400003;
}
