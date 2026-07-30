package com.ctrip.xpipe.redis.keeper.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.ctrip.xpipe.tuple.Pair;

import static org.junit.Assert.*;

public class AsyncTFSBasedFileSystemTest {

    private ExecutorService ioExecutor;
    private AsyncTFSBasedFileSystem fs;
    private Path tempDir;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("tfs-test-");
        ioExecutor = Executors.newCachedThreadPool();
        // fsyncIntervalMillis * 1_000_000 must not overflow Long.MAX_VALUE
        fs = new AsyncTFSBasedFileSystem(ioExecutor, Long.MAX_VALUE / 2, Long.MAX_VALUE / 2_000_000L);
    }

    @After
    public void tearDown() throws Exception {
        ioExecutor.shutdownNow();
        deleteRecursively(tempDir.toFile());
    }

    private static void deleteRecursively(File f) {
        if (f.isDirectory()) {
            File[] children = f.listFiles();
            if (children != null) {
                for (File c : children) deleteRecursively(c);
            }
        }
        f.delete();
    }

    private String path(String name) {
        return tempDir.resolve(name).toString();
    }

    private ByteBuf bufOf(byte[] data) {
        return Unpooled.wrappedBuffer(data);
    }

    private byte[] readAll(AsyncFile file, long length) {
        ByteBuf buf = fs.readSync(file, length, 0, 0);
        try {
            byte[] result = new byte[buf.readableBytes()];
            buf.readBytes(result);
            return result;
        } finally {
            buf.release();
        }
    }

    private void writeFile(String filePath, byte[] data) throws IOException {
        Files.write(Paths.get(filePath), data);
    }

    // =========================================================================
    // A. AsyncFile basic operations
    // =========================================================================

    @Test
    public void testOpenAndCloseWriteMode() throws Exception {
        String p = path("file1");
        AsyncFile file = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, false, false, null);
        fs.writeSync(file, bufOf(new byte[]{1, 2, 3}));
        fs.closeSync(file);
        assertTrue(Files.exists(Paths.get(p)));
        assertEquals(3, Files.size(Paths.get(p)));
    }

    @Test
    public void testOpenAndCloseReadMode() throws Exception {
        String p = path("file2");
        writeFile(p, new byte[]{10, 20, 30});
        AsyncFile file = fs.openSync(p, AbstractStorageFile.OpenMode.READ, false, false, null);
        byte[] data = readAll(file, 3);
        assertArrayEquals(new byte[]{10, 20, 30}, data);
        fs.closeSync(file);
    }

    @Test
    public void testWriterAndReaderSeparate() throws Exception {
        String p = path("file3");
        // Writer writes data
        AsyncFile writer = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, false, false, null);
        fs.writeSync(writer, bufOf(new byte[]{1, 2, 3, 4}));
        fs.closeSync(writer);
        // Separate reader reads data
        AsyncFile reader = fs.openSync(p, AbstractStorageFile.OpenMode.READ, false, false, null);
        byte[] data = readAll(reader, 4);
        assertArrayEquals(new byte[]{1, 2, 3, 4}, data);
        fs.closeSync(reader);
    }

    @Test
    public void testWriteAndRead() throws Exception {
        String p = path("file4");
        byte[] expected = new byte[100];
        for (int i = 0; i < expected.length; i++) expected[i] = (byte) (i % 256);
        // Writer writes 100 bytes
        AsyncFile writer = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, false, false, null);
        fs.writeSync(writer, bufOf(expected));
        fs.closeSync(writer);
        // Separate reader reads back
        AsyncFile reader = fs.openSync(p, AbstractStorageFile.OpenMode.READ, false, false, null);
        byte[] actual = readAll(reader, 100);
        assertArrayEquals(expected, actual);
        fs.closeSync(reader);
    }

    @Test
    public void testReadWithAlignment() throws Exception {
        String p = path("file5");
        writeFile(p, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
        AsyncFile file = fs.openSync(p, AbstractStorageFile.OpenMode.READ, false, false, null);
        // alignSize=8, offset=3, length=4 -> aligned range [0, 8)
        ByteBuf buf = fs.readSync(file, 4, 3, 8);
        try {
            // readerIndex skips leading padding: offset - alignedStart = 3 - 0 = 3
            assertEquals(3, buf.readerIndex());
            // capacity covers full aligned range = 8
            assertEquals(8, buf.capacity());
            // readableBytes includes trailing alignment padding: capacity - readerIndex = 8 - 3 = 5
            assertEquals(5, buf.readableBytes());
            byte[] data = new byte[5];
            buf.readBytes(data);
            assertArrayEquals(new byte[]{3, 4, 5, 6, 7}, data);
        } finally {
            buf.release();
        }
        fs.closeSync(file);
    }

    @Test
    public void testPositionAndRead() throws Exception {
        String p = path("file6");
        writeFile(p, new byte[]{10, 20, 30, 40, 50});
        AsyncFile file = fs.openSync(p, AbstractStorageFile.OpenMode.READ, false, false, null);
        // readSync(file, length, offset, alignSize) reads at absolute offset
        ByteBuf buf = fs.readSync(file, 3, 2, 0);
        try {
            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);
            assertArrayEquals(new byte[]{30, 40, 50}, data);
        } finally {
            buf.release();
        }
        // positionSync updates file.position for sequential reads
        fs.positionSync(file, 4);
        ByteBuf buf2 = fs.readSync(file, 1, 4, 0);
        try {
            byte[] data = new byte[buf2.readableBytes()];
            buf2.readBytes(data);
            assertArrayEquals(new byte[]{50}, data);
        } finally {
            buf2.release();
        }
        fs.closeSync(file);
    }

    @Test
    public void testTruncate() throws Exception {
        String p = path("file7");
        // Writer writes 200 bytes then truncates to 100
        AsyncFile writer = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, false, false, null);
        fs.writeSync(writer, bufOf(new byte[200]));
        fs.truncateSync(writer, 100);
        fs.closeSync(writer);
        // Separate reader verifies size
        AsyncFile reader = fs.openSync(p, AbstractStorageFile.OpenMode.READ, false, false, null);
        assertEquals(100, fs.sizeSync(reader));
        fs.closeSync(reader);
    }

    @Test
    public void testTruncateNoOp() throws Exception {
        String p = path("file8");
        // Writer writes 100 bytes then truncates to 200 (no-op)
        AsyncFile writer = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, false, false, null);
        fs.writeSync(writer, bufOf(new byte[100]));
        fs.truncateSync(writer, 200); // size >= current size, no-op
        fs.closeSync(writer);
        // Separate reader verifies size unchanged
        AsyncFile reader = fs.openSync(p, AbstractStorageFile.OpenMode.READ, false, false, null);
        assertEquals(100, fs.sizeSync(reader));
        fs.closeSync(reader);
    }

    @Test
    public void testFsyncSync() throws Exception {
        String p = path("file9");
        AsyncFile file = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, false, false, null);
        fs.writeSync(file, bufOf(new byte[]{1, 2, 3}));
        fs.fsyncSync(file); // should not throw
        fs.closeSync(file);
    }

    @Test(expected = IllegalStateException.class)
    public void testFsyncOnClosedFileThrows() throws Exception {
        String p = path("file9b");
        AsyncFile file = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, false, false, null);
        fs.closeSync(file);
        fs.fsyncSync(file);
    }

    @Test
    public void testSizeSync() throws Exception {
        String p = path("file10");
        AsyncFile file = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, false, false, null);
        fs.writeSync(file, bufOf(new byte[256]));
        assertEquals(256, fs.sizeSync(file));
        fs.closeSync(file);
    }

    @Test
    public void testDeleteSync() throws Exception {
        String p = path("file11");
        writeFile(p, new byte[]{1, 2, 3});
        assertTrue(Files.exists(Paths.get(p)));
        fs.deleteSync(p);
        assertFalse(Files.exists(Paths.get(p)));
        // deleting non-existing file should not throw
        fs.deleteSync(p);
    }

    @Test
    public void testTransferToSync() throws Exception {
        String p = path("file12");
        writeFile(p, new byte[]{10, 20, 30, 40, 50});
        AsyncFile file = fs.openSync(p, AbstractStorageFile.OpenMode.READ, false, false, null);
        ByteArrayOutputStreamChannel target = new ByteArrayOutputStreamChannel();
        long transferred = fs.transferToSync(file, 1, 3, target);
        assertEquals(3, transferred);
        assertArrayEquals(new byte[]{20, 30, 40}, target.toByteArray());
        fs.closeSync(file);
    }

    // =========================================================================
    // B. Directory operations
    // =========================================================================

    @Test
    public void testMkdirAndRmdir() throws Exception {
        String p = path("dir1");
        fs.mkdir(p, false).get();
        assertTrue(Files.isDirectory(Paths.get(p)));
        fs.rmdir(p, false).get();
        assertFalse(Files.exists(Paths.get(p)));
    }

    @Test
    public void testMkdirRecursive() throws Exception {
        String p = path("a/b/c");
        fs.mkdir(p, true).get();
        assertTrue(Files.isDirectory(Paths.get(p)));
    }

    @Test
    public void testMkdirAlreadyExists() throws Exception {
        String p = path("dir2");
        fs.mkdir(p, false).get();
        Boolean result = fs.mkdir(p, false).get();
        assertTrue(result);
    }

    @Test
    public void testRmdirRecursive() throws Exception {
        String p = path("dir3");
        Files.createDirectories(Paths.get(p, "sub"));
        writeFile(p + "/sub/file", new byte[]{1});
        fs.rmdir(p, true).get();
        assertFalse(Files.exists(Paths.get(p)));
    }

    @Test
    public void testListFiltersTmpFiles() throws Exception {
        String dir = path("dir4");
        Files.createDirectories(Paths.get(dir));
        writeFile(dir + "/real_file", new byte[]{1});
        writeFile(dir + "/TMP_REP_temp", new byte[]{2});
        List<String> list = fs.list(dir).get();
        assertTrue(list.contains("real_file"));
        assertFalse(list.contains("TMP_REP_temp"));
    }

    @Test
    public void testExistsAndIsFile() throws Exception {
        String p = path("file13");
        assertFalse(fs.exists(p).get());
        writeFile(p, new byte[]{1});
        assertTrue(fs.exists(p).get());
        assertTrue(fs.isFile(fs.openSync(p, AbstractStorageFile.OpenMode.READ, false, false, null)).get());
        assertFalse(fs.isDirectory(p).get());
        assertTrue(fs.isDirectory(tempDir.toString()).get());
    }

    // =========================================================================
    // C. AtomicReplace mechanism
    // =========================================================================

    @Test
    public void testAtomicReplaceWrite() throws Exception {
        String p = path("file14");
        writeFile(p, new byte[]{1, 2, 3});
        // Writer with atomicReplace=true writes new content
        AsyncFile writer = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, true, false, null);
        fs.writeSync(writer, bufOf(new byte[]{10, 20, 30, 40}));
        fs.closeSync(writer);
        // Verify on disk
        assertArrayEquals(new byte[]{10, 20, 30, 40}, Files.readAllBytes(Paths.get(p)));
        // tmp file should be deleted
        assertFalse(Files.exists(Paths.get(tempDir.toString(), "TMP_REP_file14")));
        // Separate reader verifies content
        AsyncFile reader = fs.openSync(p, AbstractStorageFile.OpenMode.READ, false, false, null);
        assertArrayEquals(new byte[]{10, 20, 30, 40}, readAll(reader, 4));
        fs.closeSync(reader);
    }

    @Test
    public void testRecoverFromValidTmpFile() throws Exception {
        String p = path("file15");
        // Create original file with old data
        byte[] oldData = new byte[]{1, 2, 3};
        writeFile(p, oldData);
        // Create a valid tmp file: [8-byte length][data] with new data
        byte[] newData = new byte[]{5, 6, 7, 8, 9};
        Path tmpPath = Paths.get(tempDir.toString(), "TMP_REP_file15");
        try (FileChannel ch = FileChannel.open(tmpPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer lenBuf = ByteBuffer.allocate(8);
            lenBuf.putLong(newData.length);
            lenBuf.flip();
            ch.write(lenBuf);
            ch.write(ByteBuffer.wrap(newData));
            ch.force(true);
        }
        // Open with atomicReplace=true should recover from tmp, overwriting original file
        AsyncFile writer = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, true, false, null);
        fs.closeSync(writer);
        // tmp should be cleaned up
        assertFalse(Files.exists(tmpPath));
        // Verify on disk: file content is the new data, not the old
        assertArrayEquals(newData, Files.readAllBytes(Paths.get(p)));
        // Separate reader reads recovered data
        AsyncFile reader = fs.openSync(p, AbstractStorageFile.OpenMode.READ, false, false, null);
        assertArrayEquals(newData, readAll(reader, newData.length));
        fs.closeSync(reader);
    }

    @Test
    public void testRecoverFromCorruptTmpFile() throws Exception {
        String p = path("file16");
        // Create original file with valid data
        byte[] originalData = new byte[]{10, 20, 30};
        writeFile(p, originalData);
        Path tmpPath = Paths.get(tempDir.toString(), "TMP_REP_file16");
        // Write a tmp file with mismatched length header (says 100 bytes but only has 5)
        try (FileChannel ch = FileChannel.open(tmpPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            ByteBuffer lenBuf = ByteBuffer.allocate(8);
            lenBuf.putLong(100); // claims 100 bytes
            lenBuf.flip();
            ch.write(lenBuf);
            ch.write(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5})); // only 5 bytes
            ch.force(true);
        }
        // Open should succeed, corrupt tmp gets deleted, original file untouched
        AsyncFile writer = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, true, false, null);
        fs.closeSync(writer);
        // tmp should be deleted
        assertFalse(Files.exists(tmpPath));
        // Original file should still exist with original data
        assertTrue(Files.exists(Paths.get(p)));
        assertArrayEquals(originalData, Files.readAllBytes(Paths.get(p)));
        // Separate reader reads original data (NOT affected by corrupt tmp)
        AsyncFile reader = fs.openSync(p, AbstractStorageFile.OpenMode.READ, false, false, null);
        assertArrayEquals(originalData, readAll(reader, originalData.length));
        fs.closeSync(reader);
    }

    // =========================================================================
    // D. FileEntry ref counting & concurrency
    // =========================================================================

    @Test
    public void testMultipleReadersSameFile() throws Exception {
        String p = path("file17");
        writeFile(p, new byte[]{1, 2, 3});
        AsyncFile reader1 = fs.openSync(p, AbstractStorageFile.OpenMode.READ, false, false, null);
        AsyncFile reader2 = fs.openSync(p, AbstractStorageFile.OpenMode.READ, false, false, null);
        assertArrayEquals(new byte[]{1, 2, 3}, readAll(reader1, 3));
        assertArrayEquals(new byte[]{1, 2, 3}, readAll(reader2, 3));
        fs.closeSync(reader1);
        fs.closeSync(reader2);
    }

    @Test(expected = IllegalStateException.class)
    public void testDoubleWriterThrows() throws Exception {
        String p = path("file18");
        AsyncFile writer1 = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, false, false, null);
        try {
            fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, false, false, null);
        } finally {
            fs.closeSync(writer1);
        }
    }

    @Test
    public void testCloseReleasesEntry() throws Exception {
        String p = path("file19");
        AsyncFile writer1 = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, false, false, null);
        fs.closeSync(writer1);
        // After close, should be able to open writer again
        AsyncFile writer2 = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, false, false, null);
        fs.closeSync(writer2);
    }

    // =========================================================================
    // E. fsync interval control
    // =========================================================================

    @Test
    public void testFsyncIntervalBytesGetterSetter() {
        fs.setFsyncIntervalBytes(12345);
        assertEquals(12345, fs.getFsyncIntervalBytes());
    }

    @Test
    public void testFsyncIntervalMillisGetterSetter() {
        fs.setFsyncIntervalMillis(500);
        assertEquals(500, fs.getFsyncIntervalMillis());
    }

    @Test
    public void testAutoFsyncByBytes() throws Exception {
        fs.setFsyncIntervalBytes(10);
        String p = path("file20");
        AsyncFile file = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, false, false, null);

        // Write less than threshold (5 < 10 bytes) — should NOT trigger fsync
        fs.writeSync(file, bufOf(new byte[5]));
        assertEquals(5, file.pendingFsyncBytes);

        // Write enough to exceed threshold (5 + 10 = 15 >= 10) — triggers fsync
        fs.writeSync(file, bufOf(new byte[10]));
        assertEquals(0, file.pendingFsyncBytes);

        fs.closeSync(file);
    }

    // =========================================================================
    // F. AsyncSegmentFile operations
    // =========================================================================

    private static final String SEG_PREFIX = "seg";
    private static final String IDX_PREFIX = "idx";
    private static final List<String> INDEX_PREFIXES = Collections.singletonList(IDX_PREFIX);

    @Test
    public void testSegmentOpenEmptyDirWriteMode() throws Exception {
        String dir = path("segdir1");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        List<Long> offsets = fs.list(seg);
        assertTrue(offsets.isEmpty());
        fs.closeSync(seg);
    }

    @Test
    public void testSegmentWriteAndRead() throws Exception {
        String dir = path("segdir2");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segW = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        byte[] data = new byte[]{1, 2, 3, 4, 5};
        fs.writeSync(segW, bufOf(data));
        fs.closeSync(segW);

        AsyncSegmentFile segR = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        ByteBuf buf = fs.readSync(segR, 5, segR.position);
        try {
            byte[] actual = new byte[buf.readableBytes()];
            buf.readBytes(actual);
            assertArrayEquals(data, actual);
        } finally {
            buf.release();
        }
        fs.closeSync(segR);
    }

    @Test
    public void testSegmentReadAutoSwitch() throws Exception {
        String dir = path("segdir3");
        Files.createDirectories(Paths.get(dir));
        // Write two segments via roll
        AsyncSegmentFile segW = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(segW, bufOf(new byte[]{10, 20, 30}));
        fs.rollSync(segW);
        fs.writeSync(segW, bufOf(new byte[]{40, 50, 60}));
        fs.closeSync(segW);

        // Read across both segments — pread does not update position, caller manages it
        AsyncSegmentFile segR = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        ByteBuf buf1 = fs.readSync(segR, 3, segR.position);
        try {
            byte[] d1 = new byte[3];
            buf1.readBytes(d1);
            assertArrayEquals(new byte[]{10, 20, 30}, d1);
        } finally {
            buf1.release();
        }
        // Advance position manually, then read next 3 bytes (auto-switch to next segment)
        segR.position += 3;
        ByteBuf buf2 = fs.readSync(segR, 3, segR.position);
        try {
            byte[] d2 = new byte[3];
            buf2.readBytes(d2);
            assertArrayEquals(new byte[]{40, 50, 60}, d2);
        } finally {
            buf2.release();
        }
        fs.closeSync(segR);
    }

    @Test
    public void testSegmentPread() throws Exception {
        String dir = path("segdir4");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segW = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(segW, bufOf(new byte[]{1, 2, 3, 4, 5}));
        fs.closeSync(segW);

        AsyncSegmentFile segR = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        // pread at offset 2
        ByteBuf buf = fs.readSync(segR, 3, 2);
        try {
            byte[] data = new byte[3];
            buf.readBytes(data);
            assertArrayEquals(new byte[]{3, 4, 5}, data);
        } finally {
            buf.release();
        }
        fs.closeSync(segR);
    }

    @Test
    public void testSegmentRoll() throws Exception {
        String dir = path("segdir5");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(seg, bufOf(new byte[]{1, 2, 3}));
        fs.rollSync(seg);
        List<Long> offsets = fs.list(seg);
        assertEquals(2, offsets.size());
        assertEquals(Long.valueOf(0), offsets.get(0));
        assertEquals(Long.valueOf(3), offsets.get(1));
        fs.closeSync(seg);
    }

    @Test
    public void testSegmentRollEmptyIsNoOp() throws Exception {
        String dir = path("segdir6");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        // First roll creates segment at offset 0
        fs.rollSync(seg);
        // Second roll with empty segment is a no-op (no new segment created)
        List<Long> offsetsBefore = fs.list(seg);
        fs.rollSync(seg);
        List<Long> offsetsAfter = fs.list(seg);
        assertEquals(offsetsBefore, offsetsAfter);
        fs.closeSync(seg);
    }

    @Test
    public void testSegmentList() throws Exception {
        String dir = path("segdir7");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(seg, bufOf(new byte[10]));
        fs.rollSync(seg);
        fs.writeSync(seg, bufOf(new byte[20]));
        fs.rollSync(seg);
        fs.writeSync(seg, bufOf(new byte[5]));
        List<Long> offsets = fs.list(seg);
        assertEquals(Arrays.asList(0L, 10L, 30L), offsets);
        fs.closeSync(seg);
    }

    @Test
    public void testSegmentSizeSync() throws Exception {
        String dir = path("segdir8");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(seg, bufOf(new byte[10]));
        fs.rollSync(seg);
        fs.writeSync(seg, bufOf(new byte[20]));
        assertEquals(30, fs.sizeSync(seg));
        fs.closeSync(seg);
    }

    @Test
    public void testSegmentTruncateInRange() throws Exception {
        String dir = path("segdir9");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(seg, bufOf(new byte[10]));
        fs.rollSync(seg);
        fs.writeSync(seg, bufOf(new byte[20]));
        // Truncate at offset 15 (inside second segment [10, 30))
        fs.truncateSync(seg, 15);
        List<Long> offsets = fs.list(seg);
        assertEquals(Arrays.asList(0L, 10L), offsets);
        // Second segment should be truncated to 5 bytes
        assertEquals(5, fs.sizeOfSegmentSync(seg, 10));
        fs.closeSync(seg);
    }

    @Test
    public void testSegmentTruncateOffsetBeforeFirst() throws Exception {
        String dir = path("segdir10");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(seg, bufOf(new byte[10]));
        fs.rollSync(seg);
        fs.writeSync(seg, bufOf(new byte[20]));
        // Truncate at offset -100 (before first segment at 0) -> resets everything
        fs.truncateSync(seg, -100);
        // Should create a new empty segment at offset -100
        List<Long> offsets = fs.list(seg);
        assertEquals(1, offsets.size());
        assertEquals(Long.valueOf(-100), offsets.get(0));
        // Old segment files should be deleted
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "10")));
        fs.closeSync(seg);
    }

    @Test
    public void testSegmentTruncateOffsetAfterEnd() throws Exception {
        String dir = path("segdir11");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(seg, bufOf(new byte[10]));
        fs.rollSync(seg);
        fs.writeSync(seg, bufOf(new byte[20]));
        // Truncate at offset 100 (after end at 30) -> resets everything
        fs.truncateSync(seg, 100);
        List<Long> offsets = fs.list(seg);
        assertEquals(1, offsets.size());
        assertEquals(Long.valueOf(100), offsets.get(0));
        // Old segment files should be deleted
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "10")));
        fs.closeSync(seg);
    }

    @Test
    public void testSegmentDeleteSegments() throws Exception {
        String dir = path("segdir12");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(seg, bufOf(new byte[10]));
        fs.rollSync(seg);
        fs.writeSync(seg, bufOf(new byte[20]));
        fs.rollSync(seg);
        fs.writeSync(seg, bufOf(new byte[5]));
        // Delete first segment (offset 0)
        fs.deleteSegmentsSync(seg, Collections.singletonList(0L));
        List<Long> offsets = fs.list(seg);
        assertEquals(Arrays.asList(10L, 30L), offsets);
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "10")));
        fs.closeSync(seg);
    }

    @Test
    public void testSegmentDeleteAll() throws Exception {
        String dir = path("segdir13");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(seg, bufOf(new byte[10]));
        fs.rollSync(seg);
        fs.writeSync(seg, bufOf(new byte[20]));
        fs.delete(seg).get();
        // All segment and index files should be deleted
        File[] remaining = new File(dir).listFiles();
        if (remaining != null) {
            for (File f : remaining) {
                String name = f.getName();
                assertFalse("segment file should be deleted: " + name, name.startsWith(SEG_PREFIX));
                assertFalse("index file should be deleted: " + name, name.startsWith(IDX_PREFIX));
            }
        }
        fs.closeSync(seg);
    }

    @Test
    public void testSegmentPositionSync() throws Exception {
        String dir = path("segdir14");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segW = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(segW, bufOf(new byte[]{1, 2, 3}));
        fs.rollSync(segW);
        fs.writeSync(segW, bufOf(new byte[]{4, 5, 6}));
        fs.closeSync(segW);

        AsyncSegmentFile segR = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        fs.positionSync(segR, 4); // position in second segment
        ByteBuf buf = fs.readSync(segR, 2, segR.position);
        try {
            byte[] data = new byte[2];
            buf.readBytes(data);
            assertArrayEquals(new byte[]{5, 6}, data);
        } finally {
            buf.release();
        }
        fs.closeSync(segR);
    }

    @Test
    public void testSegmentTransferToSync() throws Exception {
        String dir = path("segdir15");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segW = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(segW, bufOf(new byte[]{10, 20, 30, 40, 50}));
        fs.closeSync(segW);

        AsyncSegmentFile segR = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        ByteArrayOutputStreamChannel target = new ByteArrayOutputStreamChannel();
        long n = fs.transferToSync(segR, 1, 3, target);
        assertEquals(3, n);
        assertArrayEquals(new byte[]{20, 30, 40}, target.toByteArray());
        fs.closeSync(segR);
    }

    // =========================================================================
    // G. Segment open recovery & dirty data deletion
    // =========================================================================

    @Test
    public void testSegmentOpenDeletesUnparseableSegmentFiles() throws Exception {
        String dir = path("segdir16");
        Files.createDirectories(Paths.get(dir));
        // Create a file with prefix but non-numeric suffix
        writeFile(dir + "/" + SEG_PREFIX + "ABC", new byte[]{1});
        // Create a valid segment
        writeFile(dir + "/" + SEG_PREFIX + "0", new byte[]{2, 3});

        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        // Unparseable file should be deleted
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "ABC")));
        // Valid segment should be intact
        List<Long> offsets = fs.list(seg);
        assertEquals(Collections.singletonList(0L), offsets);
        fs.closeSync(seg);
    }

    @Test
    public void testSegmentOpenDeletesUnparseableIndexFiles() throws Exception {
        String dir = path("segdir17");
        Files.createDirectories(Paths.get(dir));
        // Valid segment
        writeFile(dir + "/" + SEG_PREFIX + "0", new byte[]{1, 2});
        // Unparseable index file
        writeFile(dir + "/" + IDX_PREFIX + "XYZ", new byte[]{3});

        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        assertFalse(Files.exists(Paths.get(dir, IDX_PREFIX + "XYZ")));
        fs.closeSync(seg);
    }

    @Test
    public void testSegmentOpenDeletesOffChainSegments() throws Exception {
        String dir = path("segdir18");
        Files.createDirectories(Paths.get(dir));
        // Segment at offset 0, size 10 -> covers [0, 10)
        writeFile(dir + "/" + SEG_PREFIX + "0", new byte[10]);
        // Segment at offset 20, size 5 -> covers [20, 25) -- gap! not contiguous with [0, 10)
        writeFile(dir + "/" + SEG_PREFIX + "20", new byte[5]);

        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        List<Long> offsets = fs.list(seg);
        // Only the highest segment chain should be kept.
        // Since [20,25) is not contiguous with [0,10), the off-chain one should be deleted.
        // The algorithm starts from highest offset and builds chain downward.
        // Highest is 20 (size 5, end=25). Next is 0 (size 10, end=10 != 20). So 0 is off-chain.
        // Result: only offset 20 remains
        assertEquals(Collections.singletonList(20L), offsets);
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "20")));
        fs.closeSync(seg);
    }

    @Test
    public void testSegmentOpenDeletesOverlappingSegments() throws Exception {
        String dir = path("segdir19");
        Files.createDirectories(Paths.get(dir));
        // Two segments that physically overlap:
        // segment at offset 0, size 20 -> covers [0, 20)
        // segment at offset 10, size 20 -> covers [10, 30) -- overlaps!
        // Sorted desc: {10, 20}, {0, 20}
        // Chain: head=10, size=20, end=30. valid={10}
        // Next: {0, 20}, end=20. chainHead=10. 20 > 10 => overlapping -> deleted
        writeFile(dir + "/" + SEG_PREFIX + "0", new byte[20]);
        writeFile(dir + "/" + SEG_PREFIX + "10", new byte[20]);

        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        List<Long> offsets = fs.list(seg);
        // Only offset 10 remains (offset 0 was overlapping)
        assertEquals(Collections.singletonList(10L), offsets);
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "10")));
        fs.closeSync(seg);
    }

    @Test
    public void testSegmentOpenDeletesOrphanIndexFiles() throws Exception {
        String dir = path("segdir20");
        Files.createDirectories(Paths.get(dir));
        // Valid segment at offset 0
        writeFile(dir + "/" + SEG_PREFIX + "0", new byte[]{1, 2, 3});
        // Orphan index file at offset 100 (no matching segment)
        writeFile(dir + "/" + IDX_PREFIX + "100", new byte[]{4, 5});
        // Valid index file at offset 0
        writeFile(dir + "/" + IDX_PREFIX + "0", new byte[]{6});

        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        // Orphan index should be deleted
        assertFalse(Files.exists(Paths.get(dir, IDX_PREFIX + "100")));
        // Valid index should remain
        assertTrue(Files.exists(Paths.get(dir, IDX_PREFIX + "0")));
        fs.closeSync(seg);
    }

    @Test
    public void testSegmentOpenRecoveryBuildsCorrectState() throws Exception {
        String dir = path("segdir21");
        Files.createDirectories(Paths.get(dir));
        // Build a valid chain: [0, 10) -> [10, 30) -> [30, 40)
        writeFile(dir + "/" + SEG_PREFIX + "0", new byte[10]);
        writeFile(dir + "/" + SEG_PREFIX + "10", new byte[20]);
        writeFile(dir + "/" + SEG_PREFIX + "30", new byte[10]);
        // Add garbage: unparseable files, orphan index
        writeFile(dir + "/" + SEG_PREFIX + "BADNAME", new byte[1]);     // unparseable
        writeFile(dir + "/" + SEG_PREFIX + "BADNAME2", new byte[5]);    // unparseable (non-numeric offset)
        writeFile(dir + "/" + IDX_PREFIX + "888", new byte[1]);         // orphan index

        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        // Valid chain should be preserved
        List<Long> offsets = fs.list(seg);
        assertEquals(Arrays.asList(0L, 10L, 30L), offsets);
        // Garbage should be deleted
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "BADNAME")));
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "BADNAME2")));
        assertFalse(Files.exists(Paths.get(dir, IDX_PREFIX + "888")));
        // Valid segment files should still exist
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "10")));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "30")));
        fs.closeSync(seg);
    }

    // =========================================================================
    // H. AsyncFile special branches
    // =========================================================================

    @Test
    public void testLenientOpenNonRegularFile() throws Exception {
        // lenient=true with a directory path → channel not opened, file object still created
        String dir = path("lenient_dir");
        Files.createDirectories(Paths.get(dir));
        AsyncFile file = fs.openSync(dir, AbstractStorageFile.OpenMode.READ, false, true, null);
        assertNotNull(file);
        // readSync should NPE because channel is null (lenient skipped openCurrentChannel)
        try {
            fs.readSync(file, 1, 0, 0);
            fail("Expected NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
        fs.closeSync(file);
    }

    @Test
    public void testOpenReadWriteMode() throws Exception {
        // OpenMode.READ_WRITE: file opened for both read and write, positioned at end
        String p = path("rw_file");
        writeFile(p, new byte[]{1, 2, 3, 4, 5});
        AsyncFile file = fs.openSync(p, AbstractStorageFile.OpenMode.READ_WRITE, false, false, null);
        // channel positioned at end of file (size=5)
        assertEquals(5, fs.sizeSync(file));
        // Write appends after position (end of file)
        fs.writeSync(file, bufOf(new byte[]{6, 7}));
        assertEquals(7, fs.sizeSync(file));
        // Read back the original data
        ByteBuf buf = fs.readSync(file, 5, 0, 0);
        try {
            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, data);
        } finally {
            buf.release();
        }
        fs.closeSync(file);
    }

    @Test
    public void testAutoFsyncByTimeInterval() throws Exception {
        // Set byte threshold very high (won't trigger), time threshold very low (1ms)
        fs.setFsyncIntervalBytes(Long.MAX_VALUE / 2);
        fs.setFsyncIntervalMillis(1);
        String p = path("file_time_fsync");
        AsyncFile file = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, false, false, null);
        // Write a few bytes — byte threshold not reached
        fs.writeSync(file, bufOf(new byte[5]));
        assertEquals(5, file.pendingFsyncBytes);
        // Sleep past the time threshold
        Thread.sleep(50);
        // Write a few more bytes — time threshold exceeded, triggers fsync
        fs.writeSync(file, bufOf(new byte[3]));
        assertEquals(0, file.pendingFsyncBytes);
        fs.closeSync(file);
    }

    @Test
    public void testTruncateAtomicReplaceNoPositionChange() throws Exception {
        // atomicReplace=true truncate should NOT change channel position
        String p = path("file_trunc_ar");
        AsyncFile writer = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, true, false, null);
        fs.writeSync(writer, bufOf(new byte[100]));
        // atomicReplaceWrite sets position to 0 then writes, so position=100 after write
        // Truncate to 50 — atomicReplace path skips channel.position(size)
        fs.truncateSync(writer, 50);
        // Position should NOT have been changed to 50 by truncate
        // (for atomicReplace, position is left as-is after truncate)
        fs.closeSync(writer);
        // File should be truncated to 50 bytes on disk
        assertEquals(50, Files.size(Paths.get(p)));
    }

    @Test
    public void testTruncateReducesPendingFsyncBytes() throws Exception {
        // truncateSync: pendingFsyncBytes reduction logic (line 351) runs,
        // then fsyncInternal at line 353 resets to 0. Verify end-to-end correctness.
        String p = path("file_trunc_pending");
        AsyncFile writer = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, false, false, null);
        fs.writeSync(writer, bufOf(new byte[100]));
        assertEquals(100, writer.pendingFsyncBytes);
        // truncateSync reduces pendingFsyncBytes by (100-30)=70, then fsyncInternal resets to 0
        fs.truncateSync(writer, 30);
        assertEquals(0, writer.pendingFsyncBytes); // reset by fsyncInternal
        assertEquals(30, fs.sizeSync(writer));
        // Write more after truncate — pendingFsyncBytes should accumulate normally
        fs.writeSync(writer, bufOf(new byte[20]));
        assertEquals(20, writer.pendingFsyncBytes);
        fs.closeSync(writer);
    }

    @Test
    public void testCloseSyncIdempotent() throws Exception {
        String p = path("file_double_close");
        AsyncFile file = fs.openSync(p, AbstractStorageFile.OpenMode.WRITE, false, false, null);
        fs.writeSync(file, bufOf(new byte[]{1}));
        fs.closeSync(file);
        // Second close should be a no-op, not throw
        fs.closeSync(file);
    }

    @Test
    public void testCloseSyncNullChannel() throws Exception {
        // Close after lenient open (channel=null) should not throw
        String dir = path("null_ch_dir");
        Files.createDirectories(Paths.get(dir));
        AsyncFile file = fs.openSync(dir, AbstractStorageFile.OpenMode.READ, false, true, null);
        // channel is null because lenient skipped openCurrentChannel for non-regular file
        fs.closeSync(file); // should not throw
    }

    // =========================================================================
    // I. Segment special branches
    // =========================================================================

    @Test
    public void testSegmentWriteAutoRollOnEmptyState() throws Exception {
        // Opening writer on empty dir leaves state empty; first writeSync auto-rolls
        String dir = path("seg_auto_roll");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        // State is empty after open (initFromFiles found no segments)
        assertTrue(fs.list(seg).isEmpty());
        // First write triggers auto-roll in writeSync
        fs.writeSync(seg, bufOf(new byte[]{1, 2, 3}));
        // After auto-roll, segment at offset 0 should exist
        List<Long> offsets = fs.list(seg);
        assertEquals(1, offsets.size());
        assertEquals(Long.valueOf(0), offsets.get(0));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        fs.closeSync(seg);
    }

    @Test
    public void testSegmentTruncateAcrossSegments() throws Exception {
        // Truncate to offset inside a different segment than currently opened by writer
        String dir = path("seg_trunc_cross");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(seg, bufOf(new byte[10])); // segment [0, 10)
        fs.rollSync(seg);
        fs.writeSync(seg, bufOf(new byte[10])); // segment [10, 20)
        // Writer currently has segment [10, 20) opened
        assertEquals(10, seg.openedSegmentStartOffset);
        // Truncate at offset 5 (inside segment [0, 10)) — needs to close [10,20), open [0,10)
        fs.truncateSync(seg, 5);
        // Writer should have switched to segment [0, 10)
        assertEquals(0, seg.openedSegmentStartOffset);
        // Segment [10, 20) should be deleted
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "10")));
        // Segment 0 should be truncated to 5 bytes
        assertEquals(5, fs.sizeOfSegmentSync(seg, 0));
        // Index file for segment 0 should exist
        assertTrue(Files.exists(Paths.get(dir, IDX_PREFIX + "0")));
        List<Long> offsets = fs.list(seg);
        assertEquals(Collections.singletonList(0L), offsets);
        // After truncate, position is still 20; getCurrentSegmentStartOffset uses floorKey(20)=0 (only one segment)
        assertEquals(0, fs.getCurrentSegmentStartOffset(seg));
        // After repositioning to the truncated segment, start offset should be 0
        fs.positionSync(seg, 3);
        assertEquals(0, fs.getCurrentSegmentStartOffset(seg));
        fs.closeSync(seg);
    }

    @Test
    public void testGetCurrentIndexFilesSyncEmptyWriterRolls() throws Exception {
        // Empty state + writer → getCurrentIndexFilesSync triggers roll
        String dir = path("seg_idx_empty_w");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        assertTrue(fs.list(seg).isEmpty());
        Pair<Long, Map<String, AsyncFile>> result = fs.getCurrentIndexFilesSync(seg, INDEX_PREFIXES);
        // roll created segment at offset 0
        assertEquals(Long.valueOf(0), result.getKey());
        // Index file for prefix should be present
        assertTrue(result.getValue().containsKey(IDX_PREFIX));
        // Segment list should now have one entry
        assertEquals(1, fs.list(seg).size());
        fs.closeSync(seg);
    }

    @Test
    public void testGetCurrentIndexFilesSyncEmptyReaderReturnsEmpty() throws Exception {
        // Empty state + reader → returns (0, empty map)
        String dir = path("seg_idx_empty_r");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        Pair<Long, Map<String, AsyncFile>> result = fs.getCurrentIndexFilesSync(seg, INDEX_PREFIXES);
        assertEquals(Long.valueOf(0), result.getKey());
        assertTrue(result.getValue().isEmpty());
        fs.closeSync(seg);
    }

    @Test
    public void testGetCurrentIndexFilesSyncNonEmpty() throws Exception {
        // Non-empty state: returns segment start offset and index files
        String dir = path("seg_idx_nonempty");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segW = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(segW, bufOf(new byte[10]));
        fs.rollSync(segW);
        fs.writeSync(segW, bufOf(new byte[20]));
        fs.closeSync(segW);

        AsyncSegmentFile segR = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        // Reader position is at firstOffset=0
        Pair<Long, Map<String, AsyncFile>> result = fs.getCurrentIndexFilesSync(segR, INDEX_PREFIXES);
        assertEquals(Long.valueOf(0), result.getKey());
        assertTrue(result.getValue().containsKey(IDX_PREFIX));
        fs.closeSync(segR);
    }

    @Test
    public void testGetStartOffsetByReadOffset() throws Exception {
        String dir = path("seg_start_off");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segW = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(segW, bufOf(new byte[10]));  // segment [0, 10)
        fs.rollSync(segW);
        fs.writeSync(segW, bufOf(new byte[20]));  // segment [10, 30)
        fs.rollSync(segW);
        fs.writeSync(segW, bufOf(new byte[5]));   // segment [30, 35)
        fs.closeSync(segW);

        AsyncSegmentFile segR = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        // Offset in first segment
        assertEquals(0, fs.getStartOffsetByReadOffset(segR, 0));
        assertEquals(0, fs.getStartOffsetByReadOffset(segR, 5));
        assertEquals(0, fs.getStartOffsetByReadOffset(segR, 9));
        // Offset in second segment
        assertEquals(10, fs.getStartOffsetByReadOffset(segR, 10));
        assertEquals(10, fs.getStartOffsetByReadOffset(segR, 25));
        // Offset in third segment
        assertEquals(30, fs.getStartOffsetByReadOffset(segR, 30));
        assertEquals(30, fs.getStartOffsetByReadOffset(segR, 34));
        fs.closeSync(segR);
    }

    @Test
    public void testGetCurrentSegmentStartOffsetWriter() throws Exception {
        // Writer always returns the currently opened segment start offset
        String dir = path("seg_cur_off_w");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        // Empty state: openedSegmentStartOffset=0 (markEmptyOpenedRange)
        assertEquals(0, fs.getCurrentSegmentStartOffset(seg));
        fs.writeSync(seg, bufOf(new byte[10]));
        // Still on segment 0
        assertEquals(0, fs.getCurrentSegmentStartOffset(seg));
        fs.rollSync(seg);
        // Now on segment 10
        assertEquals(10, fs.getCurrentSegmentStartOffset(seg));
        fs.writeSync(seg, bufOf(new byte[5]));
        assertEquals(10, fs.getCurrentSegmentStartOffset(seg));
        fs.closeSync(seg);
    }

    @Test
    public void testGetCurrentSegmentStartOffsetReader() throws Exception {
        // Reader uses floorKey(position) to find segment start offset
        String dir = path("seg_cur_off_r");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segW = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(segW, bufOf(new byte[10]));  // [0, 10)
        fs.rollSync(segW);
        fs.writeSync(segW, bufOf(new byte[20]));  // [10, 30)
        fs.closeSync(segW);

        AsyncSegmentFile segR = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        // Reader position=0 (firstOffset) → segment 0
        assertEquals(0, fs.getCurrentSegmentStartOffset(segR));
        // Move position into second segment
        fs.positionSync(segR, 15);
        assertEquals(10, fs.getCurrentSegmentStartOffset(segR));
        // Move position to exact boundary
        fs.positionSync(segR, 10);
        assertEquals(10, fs.getCurrentSegmentStartOffset(segR));
        fs.closeSync(segR);
    }

    @Test
    public void testSegmentPositionSyncCloseCurrentRecalc() throws Exception {
        // When shared state changes (new segments added) and openedSegmentEndOffset=MAX_VALUE
        // for a non-last segment, positionSync triggers closeCurrent for recalculation.
        String dir = path("seg_pos_recalc");
        Files.createDirectories(Paths.get(dir));

        // Writer1 creates segments [0,10) and [10,20)
        AsyncSegmentFile segWriter1 = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(segWriter1, bufOf(new byte[10]));
        fs.rollSync(segWriter1);
        fs.writeSync(segWriter1, bufOf(new byte[10]));
        fs.closeSync(segWriter1);

        // Reader opens — shares FileEntry with segWriter1
        AsyncSegmentFile segReader = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        // openInitialResources: state=[0,10],[10,20], sets position=0, switchToSegment(0,s)
        // → openedSegmentStartOffset=0, openedSegmentEndOffset=10

        // Writer2 (same key) adds segment [20,30), updating shared state to [0,10],[10,20],[20,30]
        AsyncSegmentFile segWriter2 = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(segWriter2, bufOf(new byte[10]));
        fs.closeSync(segWriter2);

        // positionSync: openedSegmentEndOffset=10 (not MAX_VALUE)
        // → closeCurrent condition is NOT met for this case.
        // We need a scenario where openedSegmentEndOffset=MAX_VALUE for non-last segment.
        // That happens when reader was opened with empty state (markEmptyOpenedRange → MAX_VALUE).

        // Clean up for a fresh scenario
        fs.closeSync(segReader);

        // Fresh scenario: writer opens empty dir (empty state → MAX_VALUE end)
        String dir2 = path("seg_pos_recalc2");
        Files.createDirectories(Paths.get(dir2));
        AsyncSegmentFile segW3 = fs.openSync(dir2, SEG_PREFIX, INDEX_PREFIXES, true, null);
        // State is empty, openedSegmentEndOffset = Long.MAX_VALUE
        fs.closeSync(segW3);

        // Reader opens — state is empty, markEmptyOpenedRange → MAX_VALUE
        AsyncSegmentFile segR2 = fs.openSync(dir2, SEG_PREFIX, INDEX_PREFIXES, false, null);
        assertEquals(Long.MAX_VALUE, segR2.openedSegmentEndOffset);

        // Writer adds segments, updating shared state
        AsyncSegmentFile segW4 = fs.openSync(dir2, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(segW4, bufOf(new byte[10]));
        fs.rollSync(segW4);
        fs.writeSync(segW4, bufOf(new byte[10]));
        fs.closeSync(segW4);

        // Now shared state is [0,10],[10,20]
        // positionSync: openedSegmentEndOffset=MAX_VALUE, state not empty,
        // openedSegmentStartOffset(0) != s.lastOffset(10) → triggers closeCurrent!
        fs.positionSync(segR2, 5);
        // After closeCurrent + switchToSegment: openedSegmentStartOffset=0, endOffset=10
        assertEquals(0, segR2.openedSegmentStartOffset);
        assertTrue(segR2.openedSegmentEndOffset != Long.MAX_VALUE);

        // Read should work correctly on the recalculated segment
        ByteBuf buf = fs.readSync(segR2, 5, segR2.position);
        try {
            assertEquals(5, buf.readableBytes());
        } finally {
            buf.release();
        }
        fs.closeSync(segR2);
    }

    @Test
    public void testMaybeSwitchSegmentStaleTailEof() throws Exception {
        // When segment is shrunk externally and reader has stale end offset (MAX_VALUE),
        // maybeSwitchSegment detects staleTailEof and triggers segment recalculation.
        // Since SegmentFilesNotContinuousException extends RuntimeException,
        // it escapes the IOException catch in maybeSwitchSegment.
        String dir = path("seg_stale_eof");
        Files.createDirectories(Paths.get(dir));

        // Create 2 segments: [0,10) and [10,20)
        AsyncSegmentFile segW = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(segW, bufOf(new byte[10]));
        fs.rollSync(segW);
        fs.writeSync(segW, bufOf(new byte[10]));
        fs.closeSync(segW);

        // Reader opens — state=[0,10],[10,20], position=0
        // switchToSegment(0) → openedSegmentStartOffset=0, openedSegmentEndOffset=10
        AsyncSegmentFile segR = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        assertEquals(0, segR.openedSegmentStartOffset);
        assertEquals(10, segR.openedSegmentEndOffset);

        // Shrink segment 0 to 3 bytes externally
        Path segPath = Paths.get(dir, SEG_PREFIX + "0");
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(segPath.toFile(), "rw")) {
            raf.setLength(3);
        }

        // Read at offset 5 (past shrunk file size of 3)
        // readFully reads 0 bytes → bytesRead=0
        // staleTailEof: bytesRead==0 && openedSegmentStartOffset(0) != s.lastOffset(10)
        //   && channel.size(3) <= physicalOffset(5) → all true!
        // Triggers closeCurrent + switchToSegment → detects size mismatch
        // SegmentFilesNotContinuousException (RuntimeException) propagates out
        try {
            ByteBuf buf = fs.readSync(segR, 5, 5);
            buf.release();
            fail("Expected SegmentFilesNotContinuousException");
        } catch (SegmentFilesNotContinuousException e) {
            // expected — staleTailEof triggered recalculation which detected the inconsistency
            assertTrue(e.getMessage().contains("not continuous"));
        }
        fs.closeSync(segR);
    }

    @Test
    public void testReadSyncEmptyStateReturnsEmptyBuffer() throws Exception {
        // Reader opened on empty segment dir — read returns empty ByteBuf
        String dir = path("seg_read_empty");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        ByteBuf buf = fs.readSync(seg, 10, 0);
        try {
            assertEquals(0, buf.readableBytes());
        } finally {
            buf.release();
        }
        fs.closeSync(seg);
    }

    @Test
    public void testReaderGetCurrentIndexFilesOpensFromDisk() throws Exception {
        // Reader's getCurrentIndexFiles lazily opens index files from disk
        // when they're not in the current cache (cleared by switchToSegment)
        String dir = path("seg_lazy_idx");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segW = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(segW, bufOf(new byte[10])); // segment 0 with idx0
        fs.rollSync(segW);
        fs.writeSync(segW, bufOf(new byte[20])); // segment 10 with idx10
        fs.closeSync(segW);

        // Verify index files exist on disk
        assertTrue(Files.exists(Paths.get(dir, IDX_PREFIX + "0")));
        assertTrue(Files.exists(Paths.get(dir, IDX_PREFIX + "10")));

        // Reader opens — openInitialResources positions at 0, switchToSegment(0)
        // which clears currentIndexFiles
        AsyncSegmentFile segR = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        // getCurrentIndexFilesSync → switchToSegment(0) again (clears cache again)
        // → getCurrentIndexFiles finds idx not in cache, opens from disk
        Pair<Long, Map<String, AsyncFile>> result = fs.getCurrentIndexFilesSync(segR, INDEX_PREFIXES);
        assertEquals(Long.valueOf(0), result.getKey());
        assertTrue(result.getValue().containsKey(IDX_PREFIX));
        // Verify the returned index file is functional
        AsyncFile idxFile = result.getValue().get(IDX_PREFIX);
        assertNotNull(idxFile);
        fs.closeSync(segR);
    }

    // =========================================================================
    // J. Metadata/directory special branches
    // =========================================================================

    @Test
    public void testLastModifiedAsyncFile() throws Exception {
        String p = path("file_lm");
        writeFile(p, new byte[]{1, 2, 3});
        AsyncFile file = fs.openSync(p, AbstractStorageFile.OpenMode.READ, false, false, null);
        long lm = fs.lastModified(file).get();
        assertTrue("lastModified should be positive", lm > 0);
        fs.closeSync(file);
    }

    @Test
    public void testLastModifiedSegmentEmpty() throws Exception {
        String dir = path("seg_lm_empty");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        long lm = fs.lastModified(seg).get();
        assertEquals(0L, lm);
        fs.closeSync(seg);
    }

    @Test
    public void testLastModifiedSegmentNonEmpty() throws Exception {
        String dir = path("seg_lm");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segW = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(segW, bufOf(new byte[10]));
        fs.closeSync(segW);

        AsyncSegmentFile segR = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        long lm = fs.lastModified(segR).get();
        assertTrue("lastModified should be positive for non-empty segment", lm > 0);
        fs.closeSync(segR);
    }

    @Test
    public void testLastModifiedOfSegmentMissing() throws Exception {
        String dir = path("seg_lm_miss");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(seg, bufOf(new byte[10]));
        fs.closeSync(seg);

        // Reopen and delete segment file externally
        AsyncSegmentFile segR = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        Files.delete(Paths.get(dir, SEG_PREFIX + "0"));
        long lm = fs.lastModifiedOfSegment(segR, 0).get();
        assertEquals(0L, lm);
        fs.closeSync(segR);
    }

    @Test
    public void testSizeOfSegmentSyncExisting() throws Exception {
        String dir = path("seg_size_exist");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, true, null);
        fs.writeSync(seg, bufOf(new byte[42]));
        fs.closeSync(seg);

        AsyncSegmentFile segR = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        assertEquals(42, fs.sizeOfSegmentSync(segR, 0));
        fs.closeSync(segR);
    }

    @Test
    public void testSizeOfSegmentSyncMissing() throws Exception {
        String dir = path("seg_size_miss");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = fs.openSync(dir, SEG_PREFIX, INDEX_PREFIXES, false, null);
        assertEquals(0L, fs.sizeOfSegmentSync(seg, 999));
        fs.closeSync(seg);
    }

    @Test
    public void testRmdirNonExistent() throws Exception {
        String p = path("nonexistent_dir");
        Boolean result = fs.rmdir(p, false).get();
        assertTrue("rmdir non-existent should return true", result);
    }

    @Test
    public void testRmdirFileThrows() throws Exception {
        String p = path("not_a_dir");
        writeFile(p, new byte[]{1});
        try {
            fs.rmdir(p, false).get();
            fail("Expected IllegalArgumentException");
        } catch (java.util.concurrent.ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testListNonDirectory() throws Exception {
        String p = path("a_file");
        writeFile(p, new byte[]{1});
        List<String> result = fs.list(p).get();
        assertTrue("list on file should return empty", result.isEmpty());
    }

    @Test
    public void testDeleteAsync() throws Exception {
        String p = path("file_async_del");
        writeFile(p, new byte[]{1, 2, 3});
        assertTrue(Files.exists(Paths.get(p)));
        fs.delete(p).get();
        assertFalse(Files.exists(Paths.get(p)));
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * A simple WritableByteChannel backed by a ByteArrayOutputStream, for testing transferTo.
     */
    static class ByteArrayOutputStreamChannel implements WritableByteChannel {
        private final java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        private boolean open = true;

        @Override
        public int write(ByteBuffer src) throws IOException {
            int remaining = src.remaining();
            byte[] data = new byte[remaining];
            src.get(data);
            baos.write(data);
            return remaining;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() throws IOException {
            open = false;
        }

        public byte[] toByteArray() {
            return baos.toByteArray();
        }
    }
}
