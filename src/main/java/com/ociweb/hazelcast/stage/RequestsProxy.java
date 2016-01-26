package com.ociweb.hazelcast.stage;

import com.ociweb.hazelcast.HazelcastConfigurator;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * This class provides the required producer stage for the Hazelcast Client.  It receives various
 * write-requests and puts the supplied values on the Pipe read by the encoding stage.
 */
public class RequestsProxy extends PronghornStage {

    private final static Logger log = LoggerFactory.getLogger(RequestsProxy.class);

    private HazelcastConfigurator configurator;

    private Pipe<HazelcastRequestsSchema> pipe;

    public RequestsProxy(GraphManager graphManager, Pipe<HazelcastRequestsSchema> output) {
        super(graphManager, NONE, output);
        this.pipe = output;
    }

    @Override
    public void run() {
    }

    /**
     * Check to ensure there is room to write this message fragment and initialize the location
     * to receive the write.
     *
     * @param fragmentIdentifier identifies the part of the message in use.
     * @return true for a successful write, false otherwise.
     */
    public boolean tryWriteFragment(int fragmentIdentifier) {
        return PipeWriter.tryWriteFragment(pipe, fragmentIdentifier);
    }

    /**
     * Write an Int
     * @param loc
     * @param value
     */
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
