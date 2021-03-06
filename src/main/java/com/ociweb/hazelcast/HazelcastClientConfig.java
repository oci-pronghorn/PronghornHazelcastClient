package com.ociweb.hazelcast;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;

public class HazelcastClientConfig {

    // TODO: Add a likely default candidate here
    Path configFilePath;
    public HazelcastClientConfig() {
         configFilePath = FileSystems.getDefault().getPath("~", ".hz", "configFile");
    }

    public HazelcastClientConfig(String pathname) {
        configFilePath = FileSystems.getDefault().getPath(pathname);
    }

}
