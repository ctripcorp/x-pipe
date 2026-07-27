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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        Map<String, AsyncFile> result1 = fs.rollSync(seg);
        assertFalse(result1.isEmpty());
        // Second roll with empty segment returns current index files without creating new segment
        List<Long> offsetsBefore = fs.list(seg);
        Map<String, AsyncFile> result2 = fs.rollSync(seg);
        List<Long> offsetsAfter = fs.list(seg);
        assertEquals(offsetsBefore, offsetsAfter);
        assertFalse(result2.isEmpty());
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
        Map<String, AsyncFile> result = fs.truncateSync(seg, -100);
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
        Map<String, AsyncFile> result = fs.truncateSync(seg, 100);
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
