package com.ociweb.hazelcast.util;

import com.ociweb.hazelcast.stage.HazelcastRequestsSchema;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SchemaValidation {


    @Test
    public void hzRequestFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/template/HazelcastSetRequests.xml", "FROM", HazelcastRequestsSchema.FROM));
    };

    @Test
    public void hzRequestFieldsTest() {
        assertTrue(FROMValidation.testForMatchingLocators(HazelcastRequestsSchema.instance));
    }
}
