package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.gtid.GtidSet;
import org.junit.After;
import org.junit.Assert;

import org.junit.Before;
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

    @Test
    public void testSeek() throws Exception {

        IndexWriter indexWriter = new IndexWriter( tempDir, TEST_INDEX_FILE, new GtidSet(""), null);
        indexWriter.init();

        String uuid1 = "";
        for(int i = 0; i < 40 ; i++) {
            uuid1 += "a";
        }

        Integer[] offsets = new Integer[]{12, 84, 79, 129};

        for(int i = 0; i < offsets.length; i++) {
            indexWriter.append(uuid1, i, offsets[i]);
        }

        indexWriter.finish();

        // Initialize IndexReader
        indexReader = new IndexReader(tempDir, TEST_INDEX_FILE);

        // Initialize the indexReader
        indexReader.init();

        for(int i = 0; i < offsets.length; i++) {
            long val = indexReader.seek(uuid1, i);
            Assert.assertEquals((int) offsets[i], val);
        }

    }

    @Test
    public void testSeekMany() throws Exception {

        IndexWriter indexWriter = new IndexWriter(tempDir, TEST_INDEX_FILE, new GtidSet(""), null);
        indexWriter.init();

        String uuid1 = "";
        for(int i = 0; i < 40 ; i++) {
            uuid1 += "a";
        }



        Integer[] offsets = new Integer[10000];
        Random random = new Random();
        for(int i = 0; i < offsets.length; i++) {
            offsets[i] = i* 30 + (random.nextBoolean() ? 1 : -1);
        }

        for(int i = 0; i < offsets.length; i++) {
            indexWriter.append(uuid1, i, offsets[i]);
        }

        indexWriter.finish();

        // Initialize IndexReader
        indexReader = new IndexReader(tempDir, TEST_INDEX_FILE);

        // Initialize the indexReader
        indexReader.init();

        for(int i = 0; i < offsets.length; i++) {
            long val = indexReader.seek(uuid1, i);
            Assert.assertEquals((int) offsets[i], val);
        }


    }
}
