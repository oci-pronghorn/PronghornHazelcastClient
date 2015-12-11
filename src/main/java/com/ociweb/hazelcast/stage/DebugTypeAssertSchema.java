package com.ociweb.hazelcast.stage;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class DebugTypeAssertSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
        new int[]{0xc0400013,0x80000000,0x84000001,0x88000002,0x8c000003,0x90000000,0x94000001,0x98000002,0x9c000003,0xa0000000,0xa4000001,0xa8000002,0xac000003,0xb0000004,0x98000004,0xb4000005,0x98000005,0xb8000004,0xb8000005,0xc0200013},
        (short)0,
        new String[]{"DebugType","IntegerUnsigned","IntegerUnsignedOptional","IntegerSigned","IntegerSignedOptional","LongUnsigned","LongUnsignedOptional","LongSigned","LongSignedOptional","TextASCII","TextASCIIOptional","TextUTF8","TextUTF8Optional","Decimal","Decimal","DecimalOptional","DecimalOptional","ByteArray","ByteArrayOptional",null},
        new long[]{65331, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 12, 13, 13, 14, 15, 0},
        new String[]{"global",null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null},
        "DebugTypeAssert.xml",
        new long[]{2, 2, 0},
        new int[]{2, 2, 0});

    public static final DebugTypeAssertSchema instance = new DebugTypeAssertSchema();

    private DebugTypeAssertSchema() {
        super(FROM);
    }

    public static final int MSG_DEBUGTYPE_65331 = 0x0;
    public static final int MSG_DEBUGTYPE_65331_FIELD_INTEGERUNSIGNED_0 = 0x1;
    public static final int MSG_DEBUGTYPE_65331_FIELD_INTEGERUNSIGNEDOPTIONAL_1 = 0x800002;
    public static final int MSG_DEBUGTYPE_65331_FIELD_INTEGERSIGNED_2 = 0x1000003;
    public static final int MSG_DEBUGTYPE_65331_FIELD_INTEGERSIGNEDOPTIONAL_3 = 0x1800004;
    public static final int MSG_DEBUGTYPE_65331_FIELD_LONGUNSIGNED_4 = 0x2000005;
    public static final int MSG_DEBUGTYPE_65331_FIELD_LONGUNSIGNEDOPTIONAL_5 = 0x2800007;
    public static final int MSG_DEBUGTYPE_65331_FIELD_LONGSIGNED_6 = 0x3000009;
    public static final int MSG_DEBUGTYPE_65331_FIELD_LONGSIGNEDOPTIONAL_7 = 0x380000b;
    public static final int MSG_DEBUGTYPE_65331_FIELD_TEXTASCII_8 = 0x400000d;
    public static final int MSG_DEBUGTYPE_65331_FIELD_TEXTASCIIOPTIONAL_9 = 0x480000f;
    public static final int MSG_DEBUGTYPE_65331_FIELD_TEXTUTF8_10 = 0x5000011;
    public static final int MSG_DEBUGTYPE_65331_FIELD_TEXTUTF8OPTIONAL_11 = 0x5800013;
    public static final int MSG_DEBUGTYPE_65331_FIELD_DECIMAL_12 = 0x6000015;
    public static final int MSG_DEBUGTYPE_65331_FIELD_DECIMALOPTIONAL_13 = 0x6800018;
    public static final int MSG_DEBUGTYPE_65331_FIELD_BYTEARRAY_14 = 0x700001b;
    public static final int MSG_DEBUGTYPE_65331_FIELD_BYTEARRAYOPTIONAL_15 = 0x700001d;

}
