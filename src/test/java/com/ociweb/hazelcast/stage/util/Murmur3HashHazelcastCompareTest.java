package com.ociweb.hazelcast.stage.util;


import com.hazelcast.util.HashUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Murmur3HashHazelcastCompareTest {

   private final int defaultHazelcastMurmurSeed = 0x01000193;

    @Test
    public void testMurmurHash32AgainstHazelcast() {
        byte[] testBytes = {0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B};
        int expected = HashUtil.MurmurHash3_x86_32(testBytes, 0, testBytes.length, defaultHazelcastMurmurSeed);

        int actual = Murmur3Hash.hash32(testBytes, 0, testBytes.length, defaultHazelcastMurmurSeed);
        assertEquals(expected, actual);
    }
}
