package com.ociweb.hazelcast.util;

import com.ociweb.hazelcast.stage.DebugTypeAssertSchema;
import com.ociweb.hazelcast.stage.HazelcastRequestsSchema;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;
import org.junit.Test;

import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class SchemaValidation {


    @Test
    public void hzRequestFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/HazelcastSetRequests.xml", HazelcastRequestsSchema.instance));
    };

    @Test
    public void hzRequestFieldsTest() {
        assertTrue(FROMValidation.testForMatchingLocators(HazelcastRequestsSchema.instance));
    }
}
