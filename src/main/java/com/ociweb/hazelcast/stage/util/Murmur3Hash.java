package com.ociweb.hazelcast.stage.util;

public class Murmur3Hash {
    // Based on Murmurhash 3.0 in Wikipedia and SMHasher (https://github.com/aappleby/smhasher.git
    // specifically, https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp)
    // This class hashes produced by the methods in this class are ntested to match the
    // Murmur3Hashes found in the Hazelcast com.hazelcast.util.HashUtil class.

    private static final int c1 = 0xcc9e2d51;
    private static final int c2 = 0x1b873593;
    private static final int r1 = 15;
    private static final int r2 = 13;
    private static final int m = 5;
    private static final int n = 0xe6546b64;

    @SuppressWarnings("fallthrough")
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

    private static int getByte(long[] array, int byteIdx) {
        return 0xFF & (int)(array[byteIdx>>3] >> ((byteIdx & 0x7) << 3));
    }
    private static int getByte(int[] array, int byteIdx) {
        return 0xFF & array[byteIdx>>2] >> ((byteIdx & 0x3) << 3);
    }
    private static int getByte(short[] array, int byteIdx) {
        return 0xFF & array[byteIdx>>1] >> ((byteIdx & 0x1) << 3);
    }
    private static int getByte(CharSequence csq, int byteIdx) {
        return 0xFF & csq.charAt(byteIdx>>1) >> ((byteIdx & 0x1) << 3);
    }


    /*
    public static int hash32(long[] inputArray, int inputOffset, int inputLength, int seed) {
        int offset = inputOffset*8;
        int length = inputLength*8;

        // Initialize the hash to a 'random' value
        int h = seed ^ length;

        int i = offset;
        int len = length;
        while (len >= 4) {
            int k = getByte(inputArray,i + 0);
            k |= (getByte(inputArray,i + 1)) << 8;
            k |= (getByte(inputArray,i + 2)) << 16;
            k |= (getByte(inputArray,i + 3)) << 24;

            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

            i += 4;
            len -= 4;
        }

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }

    public static int hash32(int[] inputArray, int inputOffset, int inputLength, int seed) {
        // Initialize the hash to a 'random' value
        int h = seed ^ (inputLength*4);

        int i = inputOffset;
        int len = inputLength;
        while (--len >= 0) {

            int k = inputArray[i++];

            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

        }

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }

    public static int hash32(int[] inputArray, int inputOffset, int inputLength, int inputMask, int seed) {
        // Initialize the hash to a 'random' value
        int h = seed ^ (inputLength*4);

        int i = inputOffset;
        int len = inputLength;
        while (--len >= 0) {

            int k = inputArray[inputMask & i++];

            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

        }

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }

    public static int hash32(int k, int j, int seed) {
        // Initialize the hash to a 'random' value
        int h = seed ^ (2*4);

            //first
            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

            //second
            j *= MURMUR2_MAGIC;
            j ^= j >>> MURMUR2_R;
            j *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= j;


        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }


    public static <C extends CharSequence> int hash32(C[] inputArray, int seed) {
        return hash32(inputArray, 0, inputArray.length, seed);
    }

    public static <C extends CharSequence> int hash32(C[] inputArray, int inputOffset, int inputLength, int seed) {
        // Initialize the hash to a 'random' value
        int h = seed ^ (inputLength*4);

        int i = inputOffset;
        int len = inputLength;
        while (--len >= 0) {

            int k = hash32(inputArray[i++], seed);

            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

        }

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }


    public static int hash32(short[] inputArray, int inputOffset, int inputLength, int seed) {
        int offset = inputOffset*2;
        int length = inputLength*2;

        // Initialize the hash to a 'random' value
        int h = seed ^ length;

        int i = offset;
        int len = length;
        while (len >= 4) {
            int k = getByte(inputArray,i + 0);
            k |= (getByte(inputArray,i + 1)) << 8;
            k |= (getByte(inputArray,i + 2)) << 16;
            k |= (getByte(inputArray,i + 3)) << 24;

            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

            i += 4;
            len -= 4;
        }

        switch (len) {
        case 3:
            h ^= (getByte(inputArray,i + 2)) << 16;
        case 2:
            h ^= (getByte(inputArray,i + 1)) << 8;
        case 1:
            h ^= (getByte(inputArray,i + 0));
            h *= MURMUR2_MAGIC;
        }

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }

    public static int hash32(CharSequence charSequence, int seed) {
        return null==charSequence? seed : hash32(charSequence, 0, charSequence.length(), seed);
    }

    public static int hash32(CharSequence charSequence, int inputOffset, int inputLength, int seed) {
        int offset = inputOffset*2;
        int length = inputLength*2;

        // Initialize the hash to a 'random' value
        int h = seed ^ length;

        int i = offset;
        int len = length;
        while (len >= 4) {
            int k = getByte(charSequence,i + 0);
            k |= (getByte(charSequence,i + 1)) << 8;
            k |= (getByte(charSequence,i + 2)) << 16;
            k |= (getByte(charSequence,i + 3)) << 24;

            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

            i += 4;
            len -= 4;
        }

        switch (len) {
        case 3:
            h ^= (getByte(charSequence,i + 2)) << 16;
        case 2:
            h ^= (getByte(charSequence,i + 1)) << 8;
        case 1:
            h ^= (getByte(charSequence,i + 0));
            h *= MURMUR2_MAGIC;
        }

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }


    //ByteBuffer
    @SuppressWarnings("fallthrough")
    public static int hash32(ByteBuffer src, int offset, int length, int seed) {
        // Initialize the hash to a 'random' value
        int h = seed ^ length;

        int i = offset;
        int len = length;
        while (len >= 4) {
            int k = src.get(i + 0) & 0xFF;
            k |= (src.get(i + 1) & 0xFF) << 8;
            k |= (src.get(i + 2) & 0xFF) << 16;
            k |= (src.get(i + 3) & 0xFF) << 24;

            k *= MURMUR2_MAGIC;
            k ^= k >>> MURMUR2_R;
            k *= MURMUR2_MAGIC;

            h *= MURMUR2_MAGIC;
            h ^= k;

            i += 4;
            len -= 4;
        }

        switch (len) {
        case 3:
            h ^= (src.get(i + 2) & 0xFF) << 16;
        case 2:
            h ^= (src.get(i + 1) & 0xFF) << 8;
        case 1:
            h ^= (src.get(i + 0) & 0xFF);
            h *= MURMUR2_MAGIC;
        }

        h ^= h >>> 13;
        h *= MURMUR2_MAGIC;
        h ^= h >>> 15;

        return h;
    }

    public static int hash64finalizer(long value) {
    	return hash32finalizer( ((int)(value>>32)) | (int)value );
    }

*/
    public static int hash32finalizer(int value) {
    	value ^= value >>> 16;
        value *= 0x85ebca6b;
        value ^= value >>> 13;
        value *= 0xc2b2ae35;
        value ^= value >>> 16;
        return value;
    }
}
