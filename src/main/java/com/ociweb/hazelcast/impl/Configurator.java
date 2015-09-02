package com.ociweb.hazelcast.impl;

import java.net.InetSocketAddress;

import com.ociweb.hazelcast.impl.util.InetSocketAddressImmutable;

public class Configurator {

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

}
