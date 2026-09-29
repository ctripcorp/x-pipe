package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import io.netty.buffer.ByteBuf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class BlockReaderTest {

    private BlockReader blockReader;
    private File tempFile;
    private final String uuid = "test-uuid";
    private final long initialGno = 0;
    private final int initialCmdOffset = 0;
    private final long startOffset = 0;

    @Before
    public void setUp() throws IOException {
        tempFile = Files.createTempFile("blockReaderWriterTest", ".tmp").toFile();
    }

    @After
    public void tearDown() throws IOException {
        if (tempFile != null && tempFile.exists()) {
            tempFile.delete();
        }
    }

    @Test
    public void testWriteAndRead() throws IOException {
        int[] offset = {20, 128, 356, 921};
        long endOffset;
        try (BlockEntry blockEntry = new BlockEntry(uuid, initialGno, initialCmdOffset)) {
            for (int i = 1; i <= offset.length; i++) {
                blockEntry.append(uuid, initialGno + i, offset[i - 1]);
            }
            ByteBuf buf = blockEntry.drainToByteBuf();
            try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw")) {
                byte[] bytes = new byte[buf.readableBytes()];
                buf.readBytes(bytes);
                raf.write(bytes);
                endOffset = raf.length();
            } finally {
                buf.release();
            }
        }

        blockReader = new BlockReader(startOffset, endOffset, tempFile);
        for (int i = 0; i < offset.length; i++) {
            long value = blockReader.seek(i);
            assertEquals(offset[i], value);
        }
    }
}
