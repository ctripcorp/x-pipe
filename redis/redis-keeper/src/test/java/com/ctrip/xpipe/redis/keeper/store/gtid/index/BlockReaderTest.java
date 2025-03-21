package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class BlockReaderTest {

    private BlockWriter blockWriter;
    private BlockReader blockReader;
    private File tempFile;
    private FileChannel fileChannel;
    private final String uuid = "test-uuid";
    private final long initialGno = 0;
    private final int initialCmdOffset = 0;
    private final long startOffset = 0;

    @Before
    public void setUp() throws IOException {

        tempFile = Files.createTempFile("blockReaderWriterTest", ".tmp").toFile();

        fileChannel = FileChannel.open(tempFile.toPath());

        // Initialize BlockWriter
        blockWriter = new BlockWriter(uuid, initialGno, initialCmdOffset, tempFile.getAbsolutePath());


    }

    @After
    public void tearDown() throws IOException {
        if (fileChannel != null) {
            fileChannel.close();
        }
        if (tempFile != null && tempFile.exists()) {
            tempFile.delete();
        }
    }

    @Test
    public void testWriteAndRead() throws IOException {

        int[] offset = {20, 128, 356, 921};
        // Write new data using BlockWriter

        for(int i = 1; i <= offset.length; i++) {
            blockWriter.append(uuid, initialGno + i, offset[i-1]);
        }

        blockReader = new BlockReader(startOffset, blockWriter.getPosition(), tempFile);

        for (int i = 0; i < offset.length; i++) {
            long value = blockReader.seek(i);
           // System.out.println(value);
            assertEquals(offset[i], value);
        }

    }


}


