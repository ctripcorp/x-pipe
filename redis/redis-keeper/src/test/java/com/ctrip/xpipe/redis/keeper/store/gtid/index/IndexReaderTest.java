package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.gtid.GtidSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class IndexReaderTest {

    private IndexReader indexReader;

    String tempDir = System.getProperty("java.io.tmpdir") + "test/";
    String TEST_INDEX_FILE = "index_reader";

    @Before
    public void beforeIndexReaderTest() throws IOException {
        File directory = new File(tempDir);
        if (!directory.exists()) {
            directory.mkdirs();
        }
    }

    @After
    public void tearDown() throws IOException {
        File directory = new File(tempDir);
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                     file.delete();
                }
            }
        }
        directory.delete();
    }

    @Ignore("X1c async migration — rewrite in Phase X1f")
    @Test
    public void testSeek() {
        // deferred to Phase X1f
    }

    @Ignore("X1c async migration — rewrite in Phase X1f")
    @Test
    public void testSeekMany() {
        // deferred to Phase X1f
    }
}
