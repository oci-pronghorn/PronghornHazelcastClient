package com.ociweb.hazelcast.stage.util;


import com.hazelcast.util.HashUtil;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class Murmur3HashHazelcastCompareTest {

   private final int defaultHazelcastMurmurSeed = 0x01000193;

    @Test
    public void testMurmur3Hash32AgainstHazelcast() {
        byte[] testBytes = {0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B};
        int expected = HashUtil.MurmurHash3_x86_32(testBytes, 0, testBytes.length, defaultHazelcastMurmurSeed);

        int actual = Murmur3Hash.hash32(testBytes, 0, testBytes.length, defaultHazelcastMurmurSeed);
        assertEquals(expected, actual);
    }

    @Test
    public void testMurmur3Hash32AgainstHazelcastNoSeed() {
        byte[] testBytes = {0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B};
        int expected = HashUtil.MurmurHash3_x86_32(testBytes, 0, testBytes.length);

        int actual = Murmur3Hash.hash32(testBytes, 0, testBytes.length);
        assertEquals(expected, actual);
    }


    @Test
    public void testMurmur3HashMask32SameOrientation() {
        byte[] testData = {0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B};
        byte[] testBytes = Arrays.copyOf(testData, 16);
        byte[] expectedBytes = Arrays.copyOf(testBytes, testBytes.length * 2);
        System.arraycopy(testBytes, 0, expectedBytes, testBytes.length, testBytes.length);
        int testMask =  0x0F;

        for (int testLength = 1; testLength <= testData.length; testLength++) {
            for (int offset = 0; offset <= testData.length /*- testLength*/; offset++) {
                int expected = Murmur3Hash.hash32(expectedBytes, offset, testLength, defaultHazelcastMurmurSeed);
                int expected2 = HashUtil.MurmurHash3_x86_32(expectedBytes, offset, testLength, defaultHazelcastMurmurSeed);
                int actual = Murmur3Hash.hash32(testBytes, offset, testLength, testMask, defaultHazelcastMurmurSeed);
                assertEquals("length: " + testLength + ", offset: " + offset, expected, actual);
                assertEquals("length: " + testLength + ", offset: " + offset, expected2, actual);
            }
        }
    }


    @Test
    public void testMurmur3HashMask32PartialSplitBuffer() {
        byte[] testData = {0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B};
        byte[] testBytes = new byte[16];
        System.arraycopy(testData, 0, testBytes, 8, 8);
        System.arraycopy(testData, 8, testBytes, 0, 4);
        byte[] expectedBytes = Arrays.copyOf(testData, testBytes.length * 2);
        System.arraycopy(testData, 0, expectedBytes, testBytes.length, testData.length);
        int testMask =  0x0F;

        testSplitBuffer(testData, testBytes, expectedBytes, testMask);
    }

    @Test
    public void testMurmur3HashMask32FullSplitBuffer() {
        byte[] testData = {0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x3E, 0x3F};
        byte[] testBytes = new byte[16];
        System.arraycopy(testData, 0, testBytes, 8, 8);
        System.arraycopy(testData, 8, testBytes, 0, 8);
        byte[] expectedBytes = Arrays.copyOf(testData, testBytes.length * 2);
        System.arraycopy(testData, 0, expectedBytes, testBytes.length, testData.length);
        int testMask =  0x0F;

        testSplitBuffer(testData, testBytes, expectedBytes, testMask);
    }


    private void testSplitBuffer(byte[] testData, byte[] testBytes, byte[] expectedBytes, int testMask) {
        for (int testLength = 1; testLength <= testData.length; testLength++) {
            for (int offset = 0; offset <= testData.length /*- testLength*/; offset++) {
                int expected = Murmur3Hash.hash32(expectedBytes, offset, testLength, defaultHazelcastMurmurSeed);
                int expected2 = HashUtil.MurmurHash3_x86_32(expectedBytes, offset, testLength, defaultHazelcastMurmurSeed);
                int actual = Murmur3Hash.hash32(testBytes, offset+8, testLength, testMask, defaultHazelcastMurmurSeed);
                assertEquals("oci:length: " + testLength + ", offset: " + offset, expected, actual);
                assertEquals("hz:length: " + testLength + ", offset: " + offset, expected2, actual);
            }
        }
    }
}
