package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class BlockWriterTest {

    private BlockWriter blockWriter;
    private File tempFile;
    private final String uuid = "test-uuid";
    private final long initialGno = 0;
    private final int initialCmdOffset = 0;

    @Before
    public void setUp() throws IOException {
        tempFile = Files.createTempFile("blockWriterTest", ".tmp").toFile();
        blockWriter = new BlockWriter(uuid, initialGno, initialCmdOffset, tempFile.getAbsolutePath());
    }

    @After
    public void tearDown() throws IOException {
        if (tempFile != null && tempFile.exists()) {
            tempFile.delete();
        }
    }

    @Test
    public void testAppendAndSize() throws IOException {
        blockWriter.append(uuid, initialGno + 1, 10);
        assertEquals(1, blockWriter.getSize());

        blockWriter.append(uuid, initialGno + 2, 20);
        assertEquals(2, blockWriter.getSize());
    }

    @Test
    public void testGetPosition() throws IOException {
        long initialPosition = blockWriter.getPosition();
        blockWriter.append(uuid, initialGno + 1, 10);
        long position = blockWriter.getPosition();
        assertEquals(position, initialPosition + 1);
        blockWriter.append(uuid, initialGno + 3, 127 + 10);
        position = blockWriter.getPosition();
        assertEquals(position, initialPosition + 2);
        blockWriter.append(uuid, initialGno + 3, 127 + 10 + 128);
        position = blockWriter.getPosition();
        // 超过127 2个字节
        assertEquals(position, initialPosition + 4);

    }

}
