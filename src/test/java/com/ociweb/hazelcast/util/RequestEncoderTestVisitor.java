package com.ociweb.hazelcast.util;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class RequestEncoderTestVisitor implements StreamingReadVisitor {

    private final static long ID_CORRELATIONID = 0x1ffff0;
    private final static long ID_PARTITIONHASH = 0x1fffef;

    private final static Logger log = LoggerFactory.getLogger(RequestEncoderTestVisitor.class);

    private static Pipe<RawDataSchema> output;

    private FieldReferenceOffsetManager from;
    private int maxFragmentSize;
    private int bytePos;
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
        System.out.println("TemplateOpen name:" + name);
        System.out.println("TemplateOpen id:" + id);
        if (Pipe.hasRoomForWrite(output, maxFragmentSize)) {
            bytePos = Pipe.bytesWorkingHeadPosition(output);
            startBytePos = bytePos;
            outBuffer = Pipe.byteBuffer(output);
            byteMask = Pipe.blobMask(output);
        }
    }

	@Override
	public void visitTemplateClose(String name, long id) {
        System.out.println("TemplateClose name:" + name);
        System.out.println("TemplateClose id:" + id);
        // Publish the output pipe contents
        Pipe.addBytePosAndLenSpecial(output, startBytePos, bytePos-startBytePos);
        Pipe.publishWrites(output);
	}

	@Override
	public void visitFragmentOpen(String name, long id, int cursor) {
        System.out.println("FragmentOpen:" + name);
	}

	@Override
	public void visitFragmentClose(String name, long id) {
        System.out.println("FragmentClose::" + name);
	}

	@Override
	public void visitSequenceOpen(String name, long id, int length) {
        System.out.println("SequenceOpen:" + name);
	}

	@Override
	public void visitSequenceClose(String name, long id) {
        System.out.println("SequenceClose:" + name);
	}

	@Override
	public void visitSignedInteger(String name, long id, int value) {
        System.out.println("SignedInteger:" + name);
        bytePos = writeInt32(value, bytePos, outBuffer, byteMask);
	}

	@Override
	public void visitUnsignedInteger(String name, long id, long value) {
        System.out.println("UnSignedInteger name:" + name);
        System.out.println("UnSignedInteger id:" + id);
        System.out.println("UnSignedInteger value:" + value);
        bytePos = writeInt32((int)value, bytePos, outBuffer, byteMask);
	}

	@Override
	public void visitSignedLong(String name, long id, long value) {
        System.out.println("SignedLong name :" + name);
        System.out.println("SignedLong id:" + id);
        System.out.println("SignedLong value:" + value);
        bytePos = writeInt64(value, bytePos, outBuffer, byteMask);
    }

	@Override
	public void visitUnsignedLong(String name, long id, long value) {
        System.out.println("UnSignedLong name :" + name);
        System.out.println("UnSignedLong id:" + id);
        System.out.println("UnSignedLong value:" + value);
	}

	@Override
	public void visitDecimal(String name, long id, int exp, long mant) {
        System.out.println("Decimal name :" + name);
        System.out.println("Decimal id:" + id);
        System.out.println("Decimal exp:" + exp);
        System.out.println("Decimal mant:" + mant);
	}

    @Override
    public Appendable targetUTF8(String name, long id) {
        System.out.println("targetUTF8 name :" + name);
        System.out.println("targetUTF8 id:" + name);
        tempStringBuilder.setLength(0);
        return tempStringBuilder;
    }

    @Override
	public void visitUTF8(String name, long id, Appendable value) {
        System.out.println("visitUTF8 name :" + name);
        System.out.println("visitUTF8 id:" + id);
        System.out.println("visitUTF8 value:" + value);
        int len = value.toString().length();
        bytePos = writeInt32(len, bytePos, outBuffer, byteMask);
        System.arraycopy(value.toString().getBytes(), 0, outBuffer, bytePos, len);
        bytePos += len;
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
