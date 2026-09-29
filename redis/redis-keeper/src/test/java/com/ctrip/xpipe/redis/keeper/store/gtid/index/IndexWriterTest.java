package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class IndexWriterTest {

    File tempFile;
    @Before
    public void setUp() throws IOException {
        tempFile = Files.createTempFile("IndexWriterTest", ".tmp").toFile();
    }

    @After
    public void tearDown() throws IOException {
        if (tempFile != null && tempFile.exists()) {
            String path = tempFile.toPath().toAbsolutePath().toString();
            Files.deleteIfExists(tempFile.toPath());
            Files.deleteIfExists(Paths.get(path + AbstractIndex.INDEX));
            Files.deleteIfExists(Paths.get(path + AbstractIndex.BLOCK));
        }
    }

    @Ignore("X1c async migration — rewrite in Phase X1f")
    @Test
    public void testWriter() {
        // deferred to Phase X1f
    }


}
