package com.ociweb.hazelcast.stage.util;

/**
 * TODO: Add JavaDoc for class.
 */
public class LittleEndianByteHelpers {

    public static int writeInt32(int value, int bytePos, byte[] byteBuffer, int byteMask) {
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>8));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>16));
        byteBuffer[byteMask & bytePos++] = (byte)(0xFF&(value>>24));
        return bytePos;
    }

    public static int writeInt32(int value, int bytePos, byte[] byteBuffer) {
        return writeInt32(value, bytePos, byteBuffer, Integer.MAX_VALUE);
    }

    public static int writeInt64(long value, int bytePos, byte[] byteBuffer, int byteMask) {
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
