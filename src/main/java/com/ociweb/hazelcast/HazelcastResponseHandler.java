package com.ociweb.hazelcast;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * TODO: Add JavaDoc for class.
 */
public class HazelcastResponseHandler implements HazelcastResponse {

    private int size = -1;

    @Override
    public boolean result(boolean error, int correlationId, int token, DataInputStream stream) {
        try {
            // reads whatever this needs from stream
            if (!error && correlationId == 2) {
                HazelcastResponseHandler.this.setSize(stream.readInt());
            }
        } catch (IOException ioe) {
            // TODO: handle the stream read exception
        }
        return true;
    }

    private void setSize(int newSize) {
        this.size = newSize;
    }
}
