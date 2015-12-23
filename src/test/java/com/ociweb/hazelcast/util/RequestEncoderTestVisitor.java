package com.ociweb.hazelcast.util;

import com.ociweb.hazelcast.stage.util.LittleEndianByteHelpers;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class RequestEncoderTestVisitor implements StreamingReadVisitor {

    private final static byte BIT_FLAG_START = (byte)0x80;
    private final static byte BIT_FLAG_END = (byte)0x40;
    private final static long ID_CORRELATIONID = 0x1ffff0;
    private final static long ID_PARTITIONHASH = 0x1fffef;

    private final static Logger log = LoggerFactory.getLogger(RequestEncoderTestVisitor.class);

    private static Pipe<RawDataSchema> output;

    private FieldReferenceOffsetManager from;
    private int maxFragmentSize;
    private int bytePos;
    private int rawDataMessageSize;
    int startBytePos;
    ByteBuffer inBuffer;
    byte[] outBuffer;
    int byteMask;

	StringBuilder tempStringBuilder =  new StringBuilder(128);
	ByteBuffer tempByteBuffer = ByteBuffer.allocate(1024);

	RequestEncoderTestVisitor(Pipe<RawDataSchema> output) {
        this.output = output;
        this.from = Pipe.from(output);
        this.maxFragmentSize = FieldReferenceOffsetManager.maxFragmentSize(this.from);
        // Where to set reasonable BytePos initial value?
        bytePos = 0;
    }

	@Override
	public boolean paused() {
		return !Pipe.hasRoomForWrite(output);
	}

	@Override
	public void visitTemplateOpen(String name, long id) {
        // Beginning of message

        bytePos = Pipe.bytesWorkingHeadPosition(output);
        startBytePos = bytePos;
        outBuffer = Pipe.byteBuffer(output);
        byteMask = Pipe.blobMask(output);

        rawDataMessageSize = Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);

        // Hazelcast requires 4 byte length before the packet.  This value is NOT written here on the front of the
        // packet instead it is in the fixed length section,  On socket transmit it will be sent first.
        outBuffer[byteMask & bytePos++] = 1;  //version 1 byte const
        outBuffer[byteMask & bytePos++] = BIT_FLAG_START | BIT_FLAG_END;  //flags   1 byte  begin/end  zeros

        outBuffer[byteMask & bytePos++] = (byte) (0xFF & id); //type 2 bytes (this is the messageId)
        outBuffer[byteMask & bytePos++] = (byte) (0xFF & (id >> 8));

    }

	@Override
	public void visitTemplateClose(String name, long id) {
        // Publish the output pipe contents
        int writeLen = bytePos-startBytePos;
        Pipe.addAndGetBytesWorkingHeadPosition(output, writeLen);
        Pipe.addBytePosAndLenSpecial(output, startBytePos, writeLen);
        Pipe.confirmLowLevelWrite(output, rawDataMessageSize);
        Pipe.publishWrites(output);
	}

	@Override
	public void visitFragmentOpen(String name, long id, int cursor) {
	}

	@Override
	public void visitFragmentClose(String name, long id) {
	}

	@Override
	public void visitSequenceOpen(String name, long id, int length) {
	}

	@Override
	public void visitSequenceClose(String name, long id) {
	}

	@Override
	public void visitSignedInteger(String name, long id, int value) {
        bytePos = LittleEndianByteHelpers.writeInt32(value, bytePos, outBuffer, byteMask);
	}

	@Override
	public void visitUnsignedInteger(String name, long id, long value) {
        bytePos = LittleEndianByteHelpers.writeInt32((int)value, bytePos, outBuffer, byteMask);
        if (name.equals("PartitionHash")) {
            outBuffer[byteMask & bytePos++] = 18;  // 0x12 - 2 bytes for data offset
            outBuffer[byteMask & bytePos++] = 0;
        }
	}

	@Override
	public void visitSignedLong(String name, long id, long value) {
        bytePos = LittleEndianByteHelpers.writeInt64(value, bytePos, outBuffer, byteMask);
    }

	@Override
	public void visitUnsignedLong(String name, long id, long value) {
	}

	@Override
	public void visitDecimal(String name, long id, int exp, long mant) {
	}

    @Override
    public Appendable targetUTF8(String name, long id) {
        tempStringBuilder.setLength(0);
        return tempStringBuilder;
    }

    @Override
	public void visitUTF8(String name, long id, Appendable value) {
        CharSequence cs = (CharSequence)value;
        bytePos = encodeAsUTF8(cs, cs.length(), byteMask, outBuffer, bytePos);
	}


    @Override
	public Appendable targetASCII(String name, long id) {
        throw new UnsupportedOperationException("ASCII requested -- All text for Hazelcast MUST be UTF8 encoded.");
	}

    @Override
    public void visitASCII(String name, long id, Appendable value) {
        throw new UnsupportedOperationException("ASCII requested -- All text for Hazelcast MUST be UTF8 encoded.");
    }

	@Override
	public ByteBuffer targetBytes(String name, long id, int length) {
        bytePos = LittleEndianByteHelpers.writeInt32(length, bytePos, outBuffer, byteMask);
        return Pipe.wrappedBlobForWriting(bytePos, output);
	}

	@Override
	public void visitBytes(String name, long id, ByteBuffer value) {
        bytePos = value.position();
	}

	@Override
	public void startup() {
	}

	@Override
	public void shutdown() {
        System.err.println("RequestEncoderTestVisitor shutdown: " + System.currentTimeMillis());
    }


    private int encodeAsUTF8(CharSequence s, int len, int mask, byte[] localBuf, int pos) {
        int c = 0;
        int origPos = pos;
        pos+=4;
        while (c < len) {
            pos = Pipe.encodeSingleChar((int) s.charAt(c++), localBuf, mask, pos);
        }
        LittleEndianByteHelpers.writeInt32((pos-origPos)-4, origPos, outBuffer, byteMask);
        return pos;
    }

}
