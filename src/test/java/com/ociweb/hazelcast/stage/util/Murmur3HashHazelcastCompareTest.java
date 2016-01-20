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
    public void testMurmur3HashMask32againstMurmur3Hash32() {
        byte[] testBytes = {0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B};
        int testLength = 4;
        int testMask = testBytes.length - 1;


        // ToDo first: something is wrong with the testMask on offset 1, // FIXME: 1/19/16 
        // ToDo:
        //  -- Remove the expected bytes out to a linear array.
        // For each testLength from 1 to testBytes length
        // For each offset,
        //    -- test the arrays as they match
        //    -- march the test array down and around the "ring" array  by 1 byte
        //    -- compare for the full length of the testBytes
        for (int offset = 0; offset < testBytes.length - testLength; offset++) {
            int expected = Murmur3Hash.hash32(testBytes, offset, testLength, defaultHazelcastMurmurSeed);
            int actual = Murmur3Hash.hash32(testBytes, offset, testLength, 0xff, defaultHazelcastMurmurSeed);
            assertEquals("length: " + testLength + ", offset: " + offset, expected, actual);
        }
    }

}
