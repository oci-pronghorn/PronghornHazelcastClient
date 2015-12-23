package com.ociweb.hazelcast.stage;

import java.net.InetSocketAddress;

import com.ociweb.hazelcast.stage.util.InetSocketAddressImmutable;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class HazelcastConfigurator {

    // ToDo: Get these numbers from the configuration file.
    private int numberOfConnectionStages = 40;
    // This represents the max length of name (64 to start with) + 4 byte partition hash + 4 byte UTF vli
    private int maxMidAmbleLength = 72;

    // It's easier to think of the pipes and stages as 1-based.
    protected Pipe[] encoderToConnectionPipes = new Pipe[numberOfConnectionStages + 1];
    protected Pipe[] connectionToDecoderPipes = new Pipe[numberOfConnectionStages + 1];
    protected ConnectionStage[] connectionStage = new ConnectionStage[numberOfConnectionStages + 1];

    protected int getNumberOfConnectionStages() {
        return numberOfConnectionStages;
    }

    public InetSocketAddress buildInetSocketAddress(int stageId) {
       return new InetSocketAddressImmutable("127.0.0.1",80);
    }

    public CharSequence getUUID(int stageId) {
        return "";
    }

    public CharSequence getOwnerUUID(int stageId) {
        return "";
    }

    public boolean isCustomAuth() {
        return false;
    }

    public byte[] getCustomCredentials() {
        return null;
    }

    public CharSequence getUserName(int stageId) {
        return "dev"; //default value
    }

    public CharSequence getPassword(int stageId) {
        return "dev-pass"; //default value
    }

    public int maxClusterSize() {
        return 271;
    }

    public int getHashKeyForRingId(int ringId) {
        //can only return this after the connection stages register which node they have and which ring is coming in.
        // TODO Auto-generated method stub
        return 0;
    }

    public int getMaxMidAmble() {
        return maxMidAmbleLength;
    }
}
