package com.ociweb.hazelcast.stage.util;

import com.sun.jndi.toolkit.ctx.AtomicContext;

import java.util.concurrent.atomic.AtomicInteger;

public class CorrelationId {
    private static AtomicInteger correlationId = new AtomicInteger(0);

    public static int getNextCorrelationId() {
        int curVal, newVal;
        do {
            curVal = correlationId.get();
            newVal = (curVal + 1) % Integer.MAX_VALUE;
            if (newVal == 0) newVal = 1;
        } while (!correlationId.compareAndSet(curVal, newVal));
        return newVal;
    }

    public synchronized static void setNextValue(int nextValue) {
        if ((nextValue < 1) || (nextValue == Integer.MAX_VALUE)) {
            throw new IllegalArgumentException("nextValue parameter must be 1 to MAX_VALUE-1 inclusive; received " + nextValue);
        }
        correlationId.set(nextValue - 1);
    }
}
