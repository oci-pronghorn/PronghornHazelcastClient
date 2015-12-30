package com.ociweb.hazelcast.stage;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class RequestResponseSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc0400005,0x80000000,0x80000001,0x80000002,0xb8000000,0xc0200005},
            (short)0,
            new String[]{"Response","TypeFlags","CorrelationID","PartitionID","ByteArray",null},
            new long[]{1, 17, 18, 19, 32, 0},
            new String[]{"global",null,null,null,null,null},
            "HazelcastResponse.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});
    
    public static final int MSG_RESPONSE_1 = 0x00000000;
    public static final int MSG_RESPONSE_1_FIELD_TYPEFLAGS_17 = 0x00000001;
    public static final int MSG_RESPONSE_1_FIELD_CORRELATIONID_18 = 0x00000002;
    public static final int MSG_RESPONSE_1_FIELD_PARTITIONID_19 = 0x00000003;
    public static final int MSG_RESPONSE_1_FIELD_BYTEARRAY_32 = 0x01C00004;
    
    public static RequestResponseSchema instance = new RequestResponseSchema(FROM);
    
    private RequestResponseSchema(FieldReferenceOffsetManager from) {
        super(from);
    }

}
