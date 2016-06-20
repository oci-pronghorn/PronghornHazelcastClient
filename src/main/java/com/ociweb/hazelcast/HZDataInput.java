package com.ociweb.hazelcast;

import java.io.IOException;
import java.nio.ByteOrder;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.ociweb.hazelcast.stage.RequestResponseSchema;
import com.ociweb.pronghorn.pipe.LittleEndianDataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;

public class HZDataInput extends LittleEndianDataInputBlobReader<RequestResponseSchema> implements ObjectDataInput, Data {


    public HZDataInput(Pipe<RequestResponseSchema> pipe) {
        super(pipe);
    }

    @Override
    public byte[] readByteArray() throws IOException {

        int len = readInt();
        if (len > 0) {
            byte[] b = new byte[len];
            readFully(b);
            return b;
        }
        return new byte[0];

    }

    @Override
    public char[] readCharArray() throws IOException {

        int len = readInt();
        if (len > 0) {
            char[] values = new char[len];
            for (int i = 0; i < len; i++) {
                values[i] = readChar();
            }
            return values;
        }
        return new char[0];

    }

    @Override
    public int[] readIntArray() throws IOException {

        int len = readInt();
        if (len > 0) {
            int[] values = new int[len];
            for (int i = 0; i < len; i++) {
                values[i] = readInt();
            }
            return values;
        }
        return new int[0];

    }

    @Override
    public long[] readLongArray() throws IOException {

        int len = readInt();
        if (len > 0) {
            long[] values = new long[len];
            for (int i = 0; i < len; i++) {
                values[i] = readLong();
            }
            return values;
        }
        return new long[0];

    }

    @Override
    public double[] readDoubleArray() throws IOException {

        int len = readInt();
        if (len > 0) {
            double[] values = new double[len];
            for (int i = 0; i < len; i++) {
                values[i] = readDouble();
            }
            return values;
        }
        return new double[0];

    }

    @Override
    public float[] readFloatArray() throws IOException {

        int len = readInt();
        if (len > 0) {
            float[] values = new float[len];
            for (int i = 0; i < len; i++) {
                values[i] = readFloat();
            }
            return values;
        }
        return new float[0];

    }

    @Override
    public short[] readShortArray() throws IOException {

        int len = readInt();
        if (len > 0) {
            short[] values = new short[len];
            for (int i = 0; i < len; i++) {
                values[i] = readShort();
            }
            return values;
        }
        return new short[0];

    }

    @Override
    public Data readData() throws IOException {
       return this;
    }

    @Override
    public ClassLoader getClassLoader() {
        return getClass().getClassLoader();
    }

    @Override
    public ByteOrder getByteOrder() {
        return ByteOrder.LITTLE_ENDIAN; //TODO: not sure this is right, when do we switch from little to big endian?
    }

    //For Data interface
    @Override
    public byte[] toByteArray() {
        try {
            return readByteArray();
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }

    //For Data interface
    @Override
    public int getType() {
        throw new UnsupportedOperationException();
    }

    //For Data interface
    @Override
    public int totalSize() {
       return length();
    }

    //For Data interface
    @Override
    public int dataSize() {
        return length();
    }

    //For Data interface
    @Override
    public int getHeapCost() {
        return length();
    }

    //For Data interface
    @Override
    public int getPartitionHash() {
        return hashCode(); //because we return false from hasPartitionHash()
    }

    //For Data interface
    @Override
    public boolean hasPartitionHash() {
        return false;
    }

    @Override
    public long hash64() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isPortable() {
        return false;
    }



}
