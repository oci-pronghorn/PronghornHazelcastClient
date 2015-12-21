package com.ociweb.hazelcast;

import java.io.DataInputStream;

/**
 * TODO: Add JavaDoc for class.
 */
public interface HazelcastResponse {
    public boolean result(boolean errorIndicator, int correlationId, int token, DataInputStream stream);
}
