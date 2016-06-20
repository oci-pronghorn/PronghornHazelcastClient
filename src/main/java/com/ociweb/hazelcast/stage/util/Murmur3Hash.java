package com.ociweb.hazelcast.stage.util;

/**
 * Based on Murmurhash 3.0 in Wikipedia and SMHasher (https://github.com/aappleby/smhasher.git
 * specifically, https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp)
 * The hashes produced by the methods in this class are tested to match the Murmur3Hashes
 * found in the Hazelcast com.hazelcast.util.HashUtil class.
 */
public class Murmur3Hash {

    private static final int c1 = 0xcc9e2d51;
    private static final int c2 = 0x1b873593;
    private static final int r1 = 15;
    private static final int r2 = 13;
    private static final int m = 5;
    private static final int n = 0xe6546b64;
    private static final int defaultHazelcastMurmurSeed = 0x01000193;


    /**
     * Generate a 32-bit Murmur3 hash using the default Hazelcast Murmur3 seed.
     * @param src a byte array containing the source bytes for the hash.
     * @param offset is the location of where to start reading the bytes.
     * @param length is the number of bytes to read.
     * @return a 32-bit hash.
     */
    public static int hash32(byte[] src, int offset, int length) {
        return hash32(src, offset, length, defaultHazelcastMurmurSeed);
    }

    /**
     * Generate a 32-bit Murmur3 hash.
     * @param src a byte array containing the source bytes for the hash.
     * @param offset is the location of where to start reading the bytes.
     * @param length is the number of bytes to read.
     * @param seed is the seed to use for the hash.
     * @return a 32-bit hash.
     */
    public static int hash32(byte[] src, int offset, int length, int seed) {
        int hashSize = 32;
        int h = seed;

        int i = offset;
        int len = length;
        for (; len >= 4; i += 4, len -= 4) {
            int k = src[i]   & 0xFF;
            k |= (src[i + 1] & 0xFF) << 8;
            k |= (src[i + 2] & 0xFF) << 16;
            k |= (src[i + 3] & 0xFF) << 24;

            k *= c1;
            k = (k << r1) | (k >>> (hashSize - r1));
            k *= c2;
            h ^= k;
            h = (h << r2) | (h >>> (hashSize - r2));
            h = h * m + n;
        }

        int remaining = 0;
        switch (len) {
            case 3:
                remaining ^= (src[i + 2] & 0xFF) << 16;
            case 2:
                remaining ^= (src[i + 1] & 0xFF) << 8;
            case 1:
                remaining ^= (src[i] & 0xFF);

                remaining *= c1;
                remaining = (remaining << r1) | (remaining >>> (hashSize - r1));
                remaining *= c2;
                h ^= remaining;
        }

        h ^= length;
        h = hash32finalizer(h);
        return h;
    }

    /**
     * Generate a 32-bit Murmur3 hash. This method assumes the byte[] is backed
     * by a Pronghorn Pipe Blob ring.  This means the data to be hashed may wrap
     * around the "end" of the ring.  To accomodate this possibility, the mask is
     * applied to the indexes so the wrap will occur as necessary.
     * @param src a byte array containing the source bytes for the hash.
     * @param offset is the location of where to start reading the bytes.
     * @param length is the number of bytes to read.
     * @param mask is the Pronghorn Pipe mask to apply to the
     * @param seed is the seed to use for the hash.
     * @return a 32-bit hash.
     */
    public static int hash32(byte[] src, int offset, int length, int mask, int seed) {
        int hashSize = 32;
        int h = seed;

        int i = offset;
        int len = length;
        for (; len >= 4; i += 4, len -= 4) {
            int k = src[mask & i] & 0xFF;
            k |= (src[mask & (i + 1)] & 0xFF) << 8;
            k |= (src[mask & (i + 2)] & 0xFF) << 16;
            k |= (src[mask & (i + 3)] & 0xFF) << 24;

            k *= c1;
            k = (k << r1) | (k >>> (hashSize - r1));
            k *= c2;
            h ^= k;
            h = (h << r2) | (h >>> (hashSize - r2));
            h = h * m + n;
        }

        int remaining = 0;
        switch (len) {
            case 3:
                remaining ^= (src[mask & (i + 2)] & 0xFF) << 16;
            case 2:
                remaining ^= (src[mask & (i + 1)] & 0xFF) << 8;
            case 1:
                remaining ^= (src[mask & i] & 0xFF);

                remaining *= c1;
                remaining = (remaining << r1) | (remaining >>> (hashSize - r1));
                remaining *= c2;
                h ^= remaining;
        }

        h ^= length;
        h = hash32finalizer(h);
        return h;
    }

    private static int hash32finalizer(int value) {
    	value ^= value >>> 16;
        value *= 0x85ebca6b;
        value ^= value >>> 13;
        value *= 0xc2b2ae35;
        value ^= value >>> 16;
        return value;
    }
}
