package com.ociweb.hazelcast.util;

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
		return false;
	}

	@Override
	public void visitTemplateOpen(String name, long id) {
        // Beginning of message
        System.out.println("visitor: TemplateOpen name:" + name);
        System.out.println("visitor: TemplateOpen id:" + id);
        if (Pipe.hasRoomForWrite(output, maxFragmentSize)) {
            bytePos = Pipe.bytesWorkingHeadPosition(output);
            startBytePos = bytePos;
            outBuffer = Pipe.byteBuffer(output);
            byteMask = Pipe.blobMask(output);
        }
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
        System.out.println("visitor: TemplateClose name:" + name);
        System.out.println("visitor: TemplateClose id:" + id);
        // Publish the output pipe contents
        int writeLen = bytePos-startBytePos;
        Pipe.addAndGetBytesWorkingHeadPosition(output, writeLen);
        Pipe.addBytePosAndLenSpecial(output, startBytePos, writeLen);
        Pipe.confirmLowLevelWrite(output, rawDataMessageSize);
        Pipe.publishWrites(output);
        System.out.println("visitor: Wrote: " + writeLen + " bytes on TestVisitor side.");
	}

	@Override
	public void visitFragmentOpen(String name, long id, int cursor) {
        System.out.println("visitor: FragmentOpen:" + name);
	}

	@Override
	public void visitFragmentClose(String name, long id) {
        System.out.println("visitor: FragmentClose::" + name);
	}

	@Override
	public void visitSequenceOpen(String name, long id, int length) {
        System.out.println("visitor: SequenceOpen:" + name);
	}

	@Override
	public void visitSequenceClose(String name, long id) {
        System.out.println("visitor: SequenceClose:" + name);
	}

	@Override
	public void visitSignedInteger(String name, long id, int value) {
        System.out.println("visitor: SignedInteger:" + name);
        bytePos = writeInt32(value, bytePos, outBuffer, byteMask);
	}

	@Override
	public void visitUnsignedInteger(String name, long id, long value) {
        System.out.println("visitor: UnSignedInteger name:" + name);
        System.out.println("visitor: UnSignedInteger id:" + id);
        System.out.println("visitor: UnSignedInteger value:" + value);
        System.out.println("visitor: UnSignedInteger bytePosBefore:" + bytePos);
        bytePos = writeInt32((int)value, bytePos, outBuffer, byteMask);
        System.out.println("visitor: UnSignedInteger bytePosAfter:" + bytePos);
        if (name.equals("PartitionHash")) {
            outBuffer[byteMask & bytePos++] = 18;  // 0x12 - 2 bytes for data offset
            outBuffer[byteMask & bytePos++] = 0;
        }
	}

	@Override
	public void visitSignedLong(String name, long id, long value) {
        System.out.println("visitor: SignedLong name :" + name);
        System.out.println("visitor: SignedLong id:" + id);
        System.out.println("visitor: SignedLong value:" + value);
        bytePos = writeInt64(value, bytePos, outBuffer, byteMask);
    }

	@Override
	public void visitUnsignedLong(String name, long id, long value) {
        System.out.println("visitor: UnSignedLong name :" + name);
        System.out.println("visitor: UnSignedLong id:" + id);
        System.out.println("visitor: UnSignedLong value:" + value);
	}

	@Override
	public void visitDecimal(String name, long id, int exp, long mant) {
        System.out.println("visitor: Decimal name :" + name);
        System.out.println("visitor: Decimal id:" + id);
        System.out.println("visitor: Decimal exp:" + exp);
        System.out.println("visitor: Decimal mant:" + mant);
	}

    @Override
    public Appendable targetUTF8(String name, long id) {
        System.out.println("visitor: targetUTF8 name:" + name);
        System.out.println("visitor: targetUTF8 id:" + id);
        tempStringBuilder.setLength(0);
        return tempStringBuilder;
    }

    @Override
	public void visitUTF8(String name, long id, Appendable value) {
        System.out.println("visitor: visitUTF8 name:" + name);
        System.out.println("visitor: visitUTF8 id:" + id);
        System.out.println("visitor: visitUTF8 value:" + value);

        CharSequence cs = (CharSequence)value;
        bytePos = encodeAsUTF8(cs, cs.length(), byteMask, outBuffer, bytePos);
	}

    private int encodeAsUTF8(CharSequence s, int len, int mask, byte[] localBuf, int pos) {
        int c = 0;
        int origPos = pos;
        pos+=4;
        while (c < len) {
            pos = Pipe.encodeSingleChar((int) s.charAt(c++), localBuf, mask, pos);
        }
        writeInt32((pos-origPos)-4, pos, outBuffer, byteMask);
        return pos;
    }
    

    @Override
	public Appendable targetASCII(String name, long id) {
        System.out.println("TargetASCII name:" + name);
        System.out.println("TargetASCII id:" + id);
        throw new UnsupportedOperationException("ASCII requested -- All text for Hazelcast MUST be UTF8 encoded.");
	}

    @Override
    public void visitASCII(String name, long id, Appendable value) {
        System.out.println("visitAscii name:" + name);
        System.out.println("visitAscii id:" + id);
        System.out.println("visitAscii value:" + value);
        throw new UnsupportedOperationException("ASCII requested -- All text for Hazelcast MUST be UTF8 encoded.");
    }

	@Override
	public ByteBuffer targetBytes(String name, long id, int length) {
        System.out.println("targetBytes name:" + name);
        System.out.println("targetBytes id:" + id);
        System.out.println("targetBytes length:" + length);
        tempByteBuffer.clear();
        if (tempByteBuffer.capacity() < length) {
            tempByteBuffer = ByteBuffer.allocate(length * 2);
        }
        bytePos = writeInt32(length, bytePos, outBuffer, byteMask);
        return tempByteBuffer;
	}

	@Override
	public void visitBytes(String name, long id, ByteBuffer value) {
	    value.flip();
        System.out.println("visitBytes name:" + name);
        System.out.println("visitBytes id:" + id);
        System.out.println("visitBytes value:" + value);

        System.arraycopy(value.array(), 0, outBuffer, bytePos, value.position());
        bytePos += value.position();
	}

	@Override
	public void startup() {
        System.out.println("In the startup");
	}

	@Override
	public void shutdown() {
        System.out.println("In the shutdown");
    }

    private int writeInt32(int value, int bytePos, byte[] byteBuffer, int byteMask) {
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>8));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>16));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>24));
        return bytePos;
    }


    private int writeInt64(long value, int bytePos, byte[] byteBuffer, int byteMask) {
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>8));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>16));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>24));

        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>32));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>40));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>48));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>56));
        return bytePos;
    }
}
