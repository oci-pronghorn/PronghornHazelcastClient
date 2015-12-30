package com.ociweb.hazelcast.util;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.hazelcast.stage.HazelcastRequestsSchema;
import com.ociweb.hazelcast.stage.RequestResponseSchema;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class SchemaValidationTest {


    @Test
    public void hzRequestFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/HazelcastSetRequests.xml", HazelcastRequestsSchema.instance));
    };

    @Test
    public void hzRequestFieldsTest() {
        assertTrue(FROMValidation.testForMatchingLocators(HazelcastRequestsSchema.instance));
    }
    
    @Test
    public void hzResponseFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/HazelcastResponse.xml", RequestResponseSchema.instance));
    };

    @Test
    public void hzResponseFieldsTest() {
        assertTrue(FROMValidation.testForMatchingLocators(RequestResponseSchema.instance));
    }
}
