package com.ociweb.hazelcast.stage;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Author: cas
 * Date:   1/8/16
 * Time:   13:08
 */
public class RequestsProxy extends PronghornStage {

    Pipe<HazelcastRequestsSchema> pipe;

    protected RequestsProxy(GraphManager graphManager,
                            Pipe<HazelcastRequestsSchema> output) {
        super(graphManager, NONE, output);
        this.pipe = output;
    }

    @Override
    public void run() {
    }

    public boolean tryWriteFragment(int cursorPosition) {
        return PipeWriter.tryWriteFragment(pipe, cursorPosition);
    }

    public void writeInt(int loc, int value) {
        PipeWriter.writeInt(pipe, loc, value);
    }

    public void publishWrites() {
        PipeWriter.publishWrites(pipe);
    }

    public void writeUTF8(int loc, CharSequence source) {
        PipeWriter.writeUTF8(pipe, loc, source);
    }

    public void writeBytes(int loc, byte[] source) {
        PipeWriter.writeBytes(pipe, loc, source);
    }

    public void writeBytes(int loc, ByteBuffer source) {
        PipeWriter.writeBytes(pipe, loc, source);
    }

    public void writeBytes(int loc, byte[] source, int offset, int length) {
        PipeWriter.writeBytes(pipe, loc, source, offset, length, Pipe.blobMask(pipe));
    }
}
