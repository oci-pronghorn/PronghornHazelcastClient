package com.ociweb.hazelcast.stage.util;

import org.junit.Test;

import java.util.IllegalFormatCodePointException;

import static org.junit.Assert.*;

/**
 * Author: cas
 * Date:   1/27/16
 * Time:   16:31
 */
public class CorrelationIdTest {

    @Test
    public void ensureNotSame() {
        assertNotEquals(CorrelationId.getNextCorrelationId(), CorrelationId.getNextCorrelationId());
    }

    @Test
    public void ensureIncrement() {
        int expected = 1;
        assertNotEquals(expected, CorrelationId.getNextCorrelationId());
        expected++;
        assertNotEquals(expected, CorrelationId.getNextCorrelationId());
    }

    @Test
    public void ensureSetValue() {
        int expectedValue = 47;
        CorrelationId.setNextValue(expectedValue);
        assertEquals(expectedValue, CorrelationId.getNextCorrelationId());
    }

    @Test
    public void ensureWrapWorks() {
        int expectedValue = Integer.MAX_VALUE-1;
        CorrelationId.setNextValue(expectedValue);
        assertEquals(expectedValue, CorrelationId.getNextCorrelationId());
        expectedValue = 1;
        assertEquals(expectedValue, CorrelationId.getNextCorrelationId());
    }

    @Test
    public void checkInvalidHighValue() {
        int tooHighValue = Integer.MAX_VALUE;
        try {
            CorrelationId.setNextValue(tooHighValue);
            fail("Failed to throw on invalid high end of MAX_VALUE");
        } catch (IllegalArgumentException iae) {
            String expectedMessage = "nextValue parameter must be 1 to MAX_VALUE-1 inclusive; received " + tooHighValue;
            assertEquals(expectedMessage, iae.getMessage());
        }
    }

    @Test
    public void checkInvalidLowValue() {
        int tooLowValue = 0;
        try {
            CorrelationId.setNextValue(tooLowValue);
            fail("Failed to throw on invalid low end of 0");
        } catch (IllegalArgumentException iae) {
            String expectedMessage = "nextValue parameter must be 1 to MAX_VALUE-1 inclusive; received " + tooLowValue;
            assertEquals(expectedMessage, iae.getMessage());
        }
    }
}