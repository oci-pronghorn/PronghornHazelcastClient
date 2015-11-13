package com.ociweb.hazelcast.util;

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
        System.out.println("Current directory is:" + Paths.get(".").toAbsolutePath().normalize().toString());
        System.out.println("Classloader path:" + Arrays.toString(((URLClassLoader) (Thread.currentThread().getContextClassLoader())).getURLs()));
        assertTrue(FROMValidation.testForMatchingFROMs("/HazelcastSetRequests.xml", "FROM", HazelcastRequestsSchema.FROM));
    };

    @Test
    public void hzRequestFieldsTest() {
        assertTrue(FROMValidation.testForMatchingLocators(HazelcastRequestsSchema.instance));
    }
}
