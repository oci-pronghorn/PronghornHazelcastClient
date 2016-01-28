package com.ociweb.hazelcast.stage.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class MidAmble {

    private final static Logger log = LoggerFactory.getLogger(MidAmble.class);

    // ToDo: This is a temp id location serving as a proxy for the midamble names until the tokens are put into play.
    private static Map<Integer, CharSequence> names = new HashMap<>(10);
    // Midambles are used to avoid the charsequence to utf-8 conversion as each message will carry a name.
    // This represents the max length of name (64 to start with) + 4 byte partition hash + 4 byte UTF vli
    private static int maxMidAmbleLength = 72;
    private static CharSequence[] tokenNames = new CharSequence[512];
    private static AtomicInteger token = new AtomicInteger(0);
    private static byte[] midAmbles = new byte[(getMaxMidAmble() * 50)];
    private static int penultimateMidAmbleEntry =  midAmbles.length - getMaxMidAmble();

    // Create a new token for this name
    public static int getToken(CharSequence name) {
        ByteBuffer bb = Charset.forName("UTF-8").encode(CharBuffer.wrap(name));
        byte[] midAmbleName = new byte[bb.remaining()];
        bb.get(midAmbleName);

        // Add 4 for partition Hash + 4 for UTF-8 length indicator
        int tokenLength = midAmbleName.length + 8;
        int newToken = token.getAndAdd(tokenLength);
        if (newToken > penultimateMidAmbleEntry) {
            reallocMidAmble(tokenLength, newToken);
        }
        // Put the Partition Hash for this Correlation ID in the first four bytes
        int bytePos = newToken;
        // ToDo: Create a real Partition hash to use here in place of 1
        int partitionHash = 1;
        bytePos = LittleEndianByteHelpers.writeInt32(partitionHash, bytePos, midAmbles);
        bytePos = LittleEndianByteHelpers.writeInt32(tokenLength, bytePos, midAmbles);
        System.arraycopy(midAmbleName, 0, midAmbles, bytePos, midAmbleName.length);

        // Put the position in the array in the top 16
        newToken <<= 16;
        // Put the length in the lower 16
        int returnToken = newToken + tokenLength; //TODO: This highly dangerous and only works if you know tokenLength will never be negative and will always be less than 32K
        names.put(new Integer(returnToken), name); ///TODO: This is not the right data structure, revisit.
        log.debug("Finished new Set creation for " + name);
        return returnToken;
    }

    private synchronized static void reallocMidAmble(int len, int newToken) {
        int checkToken = token.get();
        if ((checkToken == (newToken + len)) && (newToken > penultimateMidAmbleEntry)) {
            midAmbles = Arrays.copyOf(midAmbles, midAmbles.length * 2);
            penultimateMidAmbleEntry = midAmbles.length - getMaxMidAmble();
        }
    }

    public static int getMaxMidAmble() {
        return maxMidAmbleLength;
    }

    public static byte[] getMidAmbles() {
        return midAmbles;
    }

    public static void setNameForToken(int token, CharSequence name) {
        tokenNames[token] = name;
    }

    public static CharSequence getNameForToken(int token) {
        return tokenNames[token];
    }

    // TEMP!
    public static CharSequence getName(int token) {
        return names.get(token);
    }
}
