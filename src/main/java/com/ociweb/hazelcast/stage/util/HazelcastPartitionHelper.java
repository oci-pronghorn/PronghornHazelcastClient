package com.ociweb.hazelcast.stage.util;

import com.ociweb.pronghorn.pipe.util.hash.MurmurHash;
//import com.hazelcast.internal.serialization.impl.HeapData;

/**
 * The PartitionHelper provides the partition hashes (Ids) used to determine which partition will
 * contain an Object.
 */
public class HazelcastPartitionHelper {

    /**
     * Provides the Partition ID for this object.
     * @param data
     * @return the partition ID holding this object.
     */
   /*
    public static int getPartitionId(HeapData data) {
        byte[] b = data.toByteArray();
        int len = b.length;
        return MurmurHash.hash32(b, 8, len-8, 0x01000193);
    }
    */
}
