package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.gtid.GtidSet;
import org.junit.After;
import org.junit.Before;
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
            Files.deleteIfExists(Paths.get(path + IndexWriter.INDEX));
            Files.deleteIfExists(Paths.get(path + IndexWriter.BLOCK));
        }
    }

    @Test
    public void testWriter() throws Exception {

        tempFile = Files.createTempFile("indexWriter", ".tmp").toFile();
        IndexWriter indexWriter = new IndexWriter(tempFile.getParent(), tempFile.getName(), new GtidSet(""), null);
        System.out.println(tempFile.getAbsolutePath());
        indexWriter.init();
        String uuid1 ="uuid1";
        String uuid2 ="uuid2";

        for(int i = 0; i < 9000; i++) {
            indexWriter.append(uuid1, i, i * 20);
        }

        for(int i = 0; i < 9000; i++) {
            indexWriter.append(uuid2, i, i * 20 + 20 * 10000);
        }
    }


}
