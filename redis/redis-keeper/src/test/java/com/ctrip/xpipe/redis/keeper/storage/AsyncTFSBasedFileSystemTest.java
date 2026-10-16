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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

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

    // openSync only builds the file object; openWithFileEntry does the FileEntry bookkeeping,
    // the atomic replace / tmp recovery, and the channel opening. TailCacheFileSystem passes its
    // own registerInFlight/scheduleCloseChannels here; the tests drive the delegate directly,
    // so there is no in-flight registry to feed and detached channels are closed inline.
    private static final BiConsumer<String, CompletableFuture<?>> NO_REGISTER = (key, future) -> { };
    private static final BiConsumer<String, List<FileChannel>> CLOSE_CHANNELS =
            (path, channels) -> StorageUtil.closeChannels(channels);
    private static final long RECOVER_TIMEOUT_MS = 20_000;
    private static final long IO_TIMEOUT_MS = 5_000;

    private AsyncFile openFile(String filePath, AbstractStorageFile.OpenMode openMode,
            AbstractStorageFile.ReplaceMode replaceMode, boolean lenient) {
        return openFileWithInit(filePath, openMode, replaceMode, lenient).getKey();
    }

    // Same as openFile, but also reports what openWithFileEntry returned: whether the init is
    // complete. An atomic replace reader that finds a pending tmp file cannot apply the replace,
    // so it defers the recovery to the first writer and reports false.
    private Pair<AsyncFile, Boolean> openFileWithInit(String filePath, AbstractStorageFile.OpenMode openMode,
            AbstractStorageFile.ReplaceMode replaceMode, boolean lenient) {
        // key must be stable per path: the writer-exclusion and reader-sharing checks in
        // acquireFileEntry are keyed on it.
        String key = StorageUtil.asyncFileKey(filePath);
        AsyncFile file = fs.openSync(filePath, key, key, openMode, replaceMode, lenient, null, false);
        boolean initialized = fs.openWithFileEntry(file, false, NO_REGISTER, CLOSE_CHANNELS,
                RECOVER_TIMEOUT_MS, IO_TIMEOUT_MS);
        return new Pair<>(file, initialized);
    }

    private AsyncSegmentFile openSeg(String dirPath, boolean write) {
        String key = StorageUtil.segmentKey(dirPath, SEG_PREFIX);
        AsyncSegmentFile seg = fs.openSync(dirPath, SEG_PREFIX, key, key, INDEX_PREFIXES, write, null, false);
        // openWithFileEntry also runs initCurrentChannelsSync, so the tail channel is ready.
        fs.openWithFileEntry(seg, false, NO_REGISTER, CLOSE_CHANNELS,
                RECOVER_TIMEOUT_MS, IO_TIMEOUT_MS);
        return seg;
    }

    private void positionSeg(AsyncSegmentFile seg, long offset) {
        StorageUtil.closeChannels(fs.positionSync(seg, offset));
    }

    private void rollSeg(AsyncSegmentFile seg) throws IOException {
        long size = seg.currentSegmentChannel == null ? 0L : seg.currentSegmentChannel.size();
        StorageUtil.closeChannels(fs.rollMetadataSync(seg, size, false));
        fs.initCurrentChannelsSync(seg);
    }

    private void truncSeg(AsyncSegmentFile seg, long offset) {
        SegmentDirState state = fs.getSegmentDirState(seg);
        long endOffset = state.isEmpty() ? 0L : state.firstOffset + fs.sizeSync(seg);
        List<FileChannel> pending = new ArrayList<>();
        long[] dropped = fs.truncateSync(seg, offset, endOffset, false, pending);
        StorageUtil.closeChannels(pending);
        fs.initCurrentChannelsSync(seg);
        fs.truncateLastSegmentChannel(seg, offset);
        fs.deleteSegmentsIo(seg, dropped);
    }

    private void deleteSegs(AsyncSegmentFile seg, List<Long> startOffsets) {
        if (startOffsets.isEmpty()) {
            return;
        }
        long[] droppedOffsets = fs.deleteSegmentsMetadataSync(
                seg, startOffsets.get(startOffsets.size() - 1));
        fs.deleteSegmentsIo(seg, droppedOffsets);
    }

    private void deleteSeg(AsyncSegmentFile seg) {
        // Snapshot before the metadata phase empties the state; the IO phase needs the offsets.
        long[] dropped = fs.getSegmentDirState(seg).offsets();
        StorageUtil.closeChannels(fs.deleteMetadataSync(seg));
        fs.deleteSegmentsIo(seg, dropped);
    }

    private List<Long> listSeg(AsyncSegmentFile seg) {
        long[] offsets = fs.getSegmentDirState(seg).offsets();
        List<Long> result = new ArrayList<>(offsets.length);
        for (long offset : offsets) {
            result.add(offset);
        }
        return result;
    }

    private long writeSeg(AsyncSegmentFile seg, ByteBuf data) throws IOException {
        return fs.writeSync(seg, data);
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

    // Builds the TMP_REP_ sibling with the [8-byte length][data] layout the atomic replace recovery
    // expects. declaredLength goes into the header as given, so callers can forge an incomplete
    // replace by declaring more than they write.
    private Path writeTmpFile(String filePath, long declaredLength, byte[] data) throws IOException {
        Path target = Paths.get(filePath);
        Path tmpPath = target.resolveSibling("TMP_REP_" + target.getFileName());
        try (FileChannel ch = FileChannel.open(tmpPath, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer lenBuf = ByteBuffer.allocate(8);
            lenBuf.putLong(declaredLength);
            lenBuf.flip();
            ch.write(lenBuf);
            ch.write(ByteBuffer.wrap(data));
            ch.force(true);
        }
        return tmpPath;
    }

    // =========================================================================
    // A. AsyncFile basic operations
    // =========================================================================

    @Test
    public void testOpenAndCloseWriteMode() throws Exception {
        String p = path("file1");
        AsyncFile file = openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.NORMAL, false);
        fs.writeSync(file, bufOf(new byte[]{1, 2, 3}));
        StorageUtil.closeChannels(fs.closeSync(file));
        assertTrue(Files.exists(Paths.get(p)));
        assertEquals(3, Files.size(Paths.get(p)));
    }

    @Test
    public void testOpenAndCloseReadMode() throws Exception {
        String p = path("file2");
        writeFile(p, new byte[]{10, 20, 30});
        AsyncFile file = openFile(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.NORMAL, false);
        byte[] data = readAll(file, 3);
        assertArrayEquals(new byte[]{10, 20, 30}, data);
        StorageUtil.closeChannels(fs.closeSync(file));
    }

    @Test
    public void testWriteAndRead() throws Exception {
        String p = path("file4");
        byte[] expected = new byte[100];
        for (int i = 0; i < expected.length; i++) expected[i] = (byte) (i % 256);
        // Writer writes 100 bytes
        AsyncFile writer = openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.NORMAL, false);
        fs.writeSync(writer, bufOf(expected));
        StorageUtil.closeChannels(fs.closeSync(writer));
        // Separate reader reads back
        AsyncFile reader = openFile(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.NORMAL, false);
        byte[] actual = readAll(reader, 100);
        assertArrayEquals(expected, actual);
        StorageUtil.closeChannels(fs.closeSync(reader));
    }

    @Test
    public void testReadWithAlignment() throws Exception {
        String p = path("file5");
        writeFile(p, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
        AsyncFile file = openFile(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.NORMAL, false);
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
        StorageUtil.closeChannels(fs.closeSync(file));
    }

    @Test
    public void testPositionAndRead() throws Exception {
        String p = path("file6");
        writeFile(p, new byte[]{10, 20, 30, 40, 50});
        AsyncFile file = openFile(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.NORMAL, false);
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
        StorageUtil.closeChannels(fs.closeSync(file));
    }

    @Test
    public void testTruncate() throws Exception {
        String p = path("file7");
        // Writer writes 200 bytes then truncates to 100
        AsyncFile writer = openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.NORMAL, false);
        fs.writeSync(writer, bufOf(new byte[200]));
        fs.truncateSync(writer, 100);
        StorageUtil.closeChannels(fs.closeSync(writer));
        // Separate reader verifies size
        AsyncFile reader = openFile(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.NORMAL, false);
        assertEquals(100, fs.sizeSync(reader));
        StorageUtil.closeChannels(fs.closeSync(reader));
    }

    @Test
    public void testTruncateNoOp() throws Exception {
        String p = path("file8");
        // Writer writes 100 bytes then truncates to 200 (no-op)
        AsyncFile writer = openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.NORMAL, false);
        fs.writeSync(writer, bufOf(new byte[100]));
        fs.truncateSync(writer, 200); // size >= current size, no-op
        StorageUtil.closeChannels(fs.closeSync(writer));
        // Separate reader verifies size unchanged
        AsyncFile reader = openFile(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.NORMAL, false);
        assertEquals(100, fs.sizeSync(reader));
        StorageUtil.closeChannels(fs.closeSync(reader));
    }

    @Test(expected = IllegalStateException.class)
    public void testFsyncOnClosedFileThrows() throws Exception {
        String p = path("file9b");
        AsyncFile file = openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.NORMAL, false);
        StorageUtil.closeChannels(fs.closeSync(file));
        fs.fsyncSync(file);
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
        AsyncFile file = openFile(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.NORMAL, false);
        ByteArrayOutputStreamChannel target = new ByteArrayOutputStreamChannel();
        long transferred = fs.transferToSync(file, 1, 3, target);
        assertEquals(3, transferred);
        assertArrayEquals(new byte[]{20, 30, 40}, target.toByteArray());
        StorageUtil.closeChannels(fs.closeSync(file));
    }

    // =========================================================================
    // B. Directory operations
    // =========================================================================

    @Test
    public void testMkdirAndRmdir() throws Exception {
        String p = path("dir1");
        fs.mkdirSync(p, false);
        assertTrue(Files.isDirectory(Paths.get(p)));
        fs.rmdir(p, false).get();
        assertFalse(Files.exists(Paths.get(p)));
    }

    @Test
    public void testMkdirRecursive() throws Exception {
        String p = path("a/b/c");
        fs.mkdirSync(p, true);
        assertTrue(Files.isDirectory(Paths.get(p)));
    }

    @Test
    public void testMkdirAlreadyExists() throws Exception {
        String p = path("dir2");
        fs.mkdirSync(p, false);
        Boolean result = fs.mkdirSync(p, false);
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
        assertTrue(fs.isFile(openFile(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.NORMAL, false)).get());
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
        // Writer with ReplaceMode.ATOMIC writes new content
        AsyncFile writer = openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.ATOMIC, false);
        fs.writeSync(writer, bufOf(new byte[]{10, 20, 30, 40}));
        StorageUtil.closeChannels(fs.closeSync(writer));
        // Verify on disk
        assertArrayEquals(new byte[]{10, 20, 30, 40}, Files.readAllBytes(Paths.get(p)));
        // tmp file should be deleted
        assertFalse(Files.exists(Paths.get(tempDir.toString(), "TMP_REP_file14")));
        // Separate reader verifies content
        AsyncFile reader = openFile(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.NORMAL, false);
        assertArrayEquals(new byte[]{10, 20, 30, 40}, readAll(reader, 4));
        StorageUtil.closeChannels(fs.closeSync(reader));
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
        // Opening for write with ReplaceMode.ATOMIC should recover from tmp, overwriting original file
        AsyncFile writer = openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.ATOMIC, false);
        StorageUtil.closeChannels(fs.closeSync(writer));
        // tmp should be cleaned up
        assertFalse(Files.exists(tmpPath));
        // Verify on disk: file content is the new data, not the old
        assertArrayEquals(newData, Files.readAllBytes(Paths.get(p)));
        // Separate reader reads recovered data
        AsyncFile reader = openFile(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.NORMAL, false);
        assertArrayEquals(newData, readAll(reader, newData.length));
        StorageUtil.closeChannels(fs.closeSync(reader));
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
        AsyncFile writer = openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.ATOMIC, false);
        StorageUtil.closeChannels(fs.closeSync(writer));
        // tmp should be deleted
        assertFalse(Files.exists(tmpPath));
        // Original file should still exist with original data
        assertTrue(Files.exists(Paths.get(p)));
        assertArrayEquals(originalData, Files.readAllBytes(Paths.get(p)));
        // Separate reader reads original data (NOT affected by corrupt tmp)
        AsyncFile reader = openFile(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.NORMAL, false);
        assertArrayEquals(originalData, readAll(reader, originalData.length));
        StorageUtil.closeChannels(fs.closeSync(reader));
    }

    @Test
    public void testValidTmpDeferredByReaderThenRecoveredByWriter() throws Exception {
        String p = path("file15_deferred");
        byte[] oldData = new byte[]{1, 2, 3};
        byte[] newData = new byte[]{5, 6, 7, 8, 9};
        writeFile(p, oldData);
        Path tmpPath = writeTmpFile(p, newData.length, newData);

        // Applying the replace needs write permission, so the reader initializes the entry but
        // reports the init as incomplete and leaves both the tmp and the target file alone.
        Pair<AsyncFile, Boolean> openedReader =
                openFileWithInit(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.ATOMIC, false);
        AsyncFile reader = openedReader.getKey();
        assertFalse(openedReader.getValue());
        assertTrue(Files.exists(tmpPath));
        assertArrayEquals(oldData, Files.readAllBytes(Paths.get(p)));
        // Plain ATOMIC never falls back to the tmp file, so this reader sees the superseded content.
        assertArrayEquals(oldData, readAll(reader, oldData.length));

        // The writer joins the entry the reader initialized and finishes the recovery there.
        Pair<AsyncFile, Boolean> openedWriter =
                openFileWithInit(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.ATOMIC, false);
        AsyncFile writer = openedWriter.getKey();
        assertTrue(openedWriter.getValue());
        assertFalse(Files.exists(tmpPath));
        assertArrayEquals(newData, Files.readAllBytes(Paths.get(p)));

        StorageUtil.closeChannels(fs.closeSync(writer));
        StorageUtil.closeChannels(fs.closeSync(reader));
    }

    @Test
    public void testInvalidTmpLeftByReaderThenDeletedByWriter() throws Exception {
        String p = path("file16_deferred");
        byte[] originalData = new byte[]{10, 20, 30};
        writeFile(p, originalData);
        // Header claims 100 bytes but only 5 follow: an incomplete replace that must be discarded.
        Path tmpPath = writeTmpFile(p, 100, new byte[]{1, 2, 3, 4, 5});

        // There is nothing to recover, so the init is complete even for a reader. Dropping the
        // leftover still needs write permission, so the tmp file survives the reader.
        Pair<AsyncFile, Boolean> openedReader =
                openFileWithInit(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.ATOMIC, false);
        AsyncFile reader = openedReader.getKey();
        assertTrue(openedReader.getValue());
        assertTrue(Files.exists(tmpPath));
        assertArrayEquals(originalData, Files.readAllBytes(Paths.get(p)));
        assertArrayEquals(originalData, readAll(reader, originalData.length));

        // The writer drops the incomplete tmp and leaves the target file untouched.
        Pair<AsyncFile, Boolean> openedWriter =
                openFileWithInit(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.ATOMIC, false);
        AsyncFile writer = openedWriter.getKey();
        assertTrue(openedWriter.getValue());
        assertFalse(Files.exists(tmpPath));
        assertArrayEquals(originalData, Files.readAllBytes(Paths.get(p)));

        StorageUtil.closeChannels(fs.closeSync(writer));
        StorageUtil.closeChannels(fs.closeSync(reader));
    }

    // The ATOMIC_PREFER_TMP cases below need a pending tmp file to outlive the open, and only a
    // reader leaves one behind: a writer recovers and deletes it while opening.
    private AsyncFile openReaderOverPendingTmp(String filePath, byte[] oldData, byte[] newData)
            throws IOException {
        writeFile(filePath, oldData);
        writeTmpFile(filePath, newData.length, newData);
        Pair<AsyncFile, Boolean> opened = openFileWithInit(filePath, AbstractStorageFile.OpenMode.READ,
                AbstractStorageFile.ReplaceMode.ATOMIC_PREFER_TMP, false);
        assertFalse(opened.getValue());
        return opened.getKey();
    }

    @Test
    public void testReadSyncFromValidTmpWhenPreferTmp() throws Exception {
        String p = path("file_tmp_first_read");
        byte[] newData = new byte[]{5, 6, 7, 8, 9};
        AsyncFile reader = openReaderOverPendingTmp(p, new byte[]{1, 2, 3}, newData);
        try {
            // Served from the tmp file, not from the superseded target file.
            assertArrayEquals(newData, readAll(reader, newData.length));
            // Offsets stay relative to the data, so the 8-byte tmp header is invisible.
            ByteBuf buf = fs.readSync(reader, 2, 1, 0);
            try {
                byte[] mid = new byte[buf.readableBytes()];
                buf.readBytes(mid);
                assertArrayEquals(new byte[]{6, 7}, mid);
            } finally {
                buf.release();
            }
        } finally {
            StorageUtil.closeChannels(fs.closeSync(reader));
        }
    }

    @Test
    public void testSizeSyncFromValidTmpWhenPreferTmp() throws Exception {
        String p = path("file_tmp_first_size");
        AsyncFile reader = openReaderOverPendingTmp(p, new byte[]{1, 2, 3}, new byte[]{5, 6, 7, 8, 9});
        try {
            // The reported size is the pending data length, excluding the tmp header, while the
            // target file on disk still has the shorter superseded content.
            assertEquals(3, Files.size(Paths.get(p)));
            assertEquals(5, fs.sizeSync(reader));
        } finally {
            StorageUtil.closeChannels(fs.closeSync(reader));
        }
    }

    @Test
    public void testTransferToSyncFromValidTmpWhenPreferTmp() throws Exception {
        String p = path("file_tmp_first_transfer");
        AsyncFile reader = openReaderOverPendingTmp(p, new byte[]{1, 2, 3}, new byte[]{5, 6, 7, 8, 9});
        ByteArrayOutputStreamChannel target = new ByteArrayOutputStreamChannel();
        try {
            long transferred = fs.transferToSync(reader, 1, 3, target);
            assertEquals(3, transferred);
            assertArrayEquals(new byte[]{6, 7, 8}, target.toByteArray());
        } finally {
            StorageUtil.closeChannels(fs.closeSync(reader));
        }
    }

    @Test
    public void testPreferTmpOpenSucceedsWithOnlyTmpAndNullChannel() throws Exception {
        String p = path("file_prefer_tmp_only_tmp");
        byte[] newData = new byte[]{5, 6, 7, 8, 9};
        writeTmpFile(p, newData.length, newData);
        assertFalse(Files.exists(Paths.get(p)));

        AsyncFile reader = openFile(p, AbstractStorageFile.OpenMode.READ,
                AbstractStorageFile.ReplaceMode.ATOMIC_PREFER_TMP, false);
        try {
            assertNull(reader.channel);
            assertEquals(newData.length, fs.sizeSync(reader));
            assertArrayEquals(newData, readAll(reader, newData.length));
            // Served entirely from tmp; target channel stays closed.
            assertNull(reader.channel);
        } finally {
            StorageUtil.closeChannels(fs.closeSync(reader));
        }
    }

    @Test
    public void testPreferTmpLazyOpensTargetAfterRecover() throws Exception {
        String p = path("file_prefer_tmp_lazy_open");
        byte[] newData = new byte[]{5, 6, 7, 8, 9};
        writeTmpFile(p, newData.length, newData);
        assertFalse(Files.exists(Paths.get(p)));

        AsyncFile reader = openFile(p, AbstractStorageFile.OpenMode.READ,
                AbstractStorageFile.ReplaceMode.ATOMIC_PREFER_TMP, false);
        assertNull(reader.channel);
        try {
            // Writer recovers the pending tmp onto the target while the reader is still open.
            AsyncFile writer = openFile(p, AbstractStorageFile.OpenMode.WRITE,
                    AbstractStorageFile.ReplaceMode.ATOMIC, false);
            StorageUtil.closeChannels(fs.closeSync(writer));
            assertFalse(Files.exists(Paths.get(p).resolveSibling("TMP_REP_" + Paths.get(p).getFileName())));
            assertArrayEquals(newData, Files.readAllBytes(Paths.get(p)));

            assertEquals(newData.length, fs.sizeSync(reader));
            assertNotNull(reader.channel);
            // Detach so read / transferTo must lazy-open again.
            StorageUtil.closeChannels(reader.detachCurrentChannels());
            assertNull(reader.channel);
            assertArrayEquals(newData, readAll(reader, newData.length));
            assertNotNull(reader.channel);

            StorageUtil.closeChannels(reader.detachCurrentChannels());
            assertNull(reader.channel);
            ByteArrayOutputStreamChannel target = new ByteArrayOutputStreamChannel();
            assertEquals(newData.length, fs.transferToSync(reader, 0, newData.length, target));
            assertArrayEquals(newData, target.toByteArray());
            assertNotNull(reader.channel);
        } finally {
            StorageUtil.closeChannels(fs.closeSync(reader));
        }
    }

    @Test
    public void testPreferTmpIoReturnsEmptyWhenTargetAndTmpMissing() throws Exception {
        String p = path("file_prefer_tmp_missing_both");
        AsyncFile reader = openFile(p, AbstractStorageFile.OpenMode.READ,
                AbstractStorageFile.ReplaceMode.ATOMIC_PREFER_TMP, false);
        try {
            assertNull(reader.channel);
            assertEquals(0L, fs.sizeSync(reader));
            ByteBuf buf = fs.readSync(reader, 1, 0, 0);
            try {
                assertEquals(0, buf.readableBytes());
            } finally {
                buf.release();
            }
            assertEquals(0L, fs.transferToSync(reader, 0, 1, new ByteArrayOutputStreamChannel()));
            assertNull(reader.channel);
        } finally {
            StorageUtil.closeChannels(fs.closeSync(reader));
        }
    }

    @Test
    public void testReadOpenMissingFileWithoutPreferTmpStillFails() {
        String p = path("file_read_missing_no_prefer");
        assertFalse(Files.exists(Paths.get(p)));
        for (AbstractStorageFile.ReplaceMode mode : new AbstractStorageFile.ReplaceMode[]{
                AbstractStorageFile.ReplaceMode.NORMAL,
                AbstractStorageFile.ReplaceMode.ATOMIC}) {
            try {
                openFile(p, AbstractStorageFile.OpenMode.READ, mode, false);
                fail("expected open failure for missing target with " + mode);
            } catch (RuntimeException e) {
                Throwable t = e;
                while (t.getCause() != null) {
                    t = t.getCause();
                }
                assertTrue("mode=" + mode + " got " + t,
                        t instanceof java.nio.file.NoSuchFileException
                                || (t instanceof StorageIOException
                                && t.getCause() instanceof java.nio.file.NoSuchFileException));
            }
        }
    }

    // =========================================================================
    // D. FileEntry ref counting & concurrency
    // =========================================================================

    @Test
    public void testMultipleReadersSameFile() throws Exception {
        String p = path("file17");
        writeFile(p, new byte[]{1, 2, 3});
        AsyncFile reader1 = openFile(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.NORMAL, false);
        AsyncFile reader2 = openFile(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.NORMAL, false);
        assertArrayEquals(new byte[]{1, 2, 3}, readAll(reader1, 3));
        assertArrayEquals(new byte[]{1, 2, 3}, readAll(reader2, 3));
        StorageUtil.closeChannels(fs.closeSync(reader1));
        StorageUtil.closeChannels(fs.closeSync(reader2));
    }

    @Test(expected = IllegalStateException.class)
    public void testDoubleWriterThrows() throws Exception {
        String p = path("file18");
        AsyncFile writer1 = openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.NORMAL, false);
        try {
            openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.NORMAL, false);
        } finally {
            StorageUtil.closeChannels(fs.closeSync(writer1));
        }
    }

    @Test
    public void testCloseReleasesEntry() throws Exception {
        String p = path("file19");
        AsyncFile writer1 = openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.NORMAL, false);
        StorageUtil.closeChannels(fs.closeSync(writer1));
        // After close, should be able to open writer again
        AsyncFile writer2 = openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.NORMAL, false);
        StorageUtil.closeChannels(fs.closeSync(writer2));
    }

    // =========================================================================
    // E. fsync interval control
    // =========================================================================

    @Test
    public void testFsyncIntervalMillisGetterSetter() {
        fs.setFsyncIntervalMillis(500);
        assertEquals(500, fs.getFsyncIntervalMillis());
    }

    @Test
    public void testAutoFsyncByBytes() throws Exception {
        fs.setFsyncIntervalBytes(10);
        String p = path("file20");
        AsyncFile file = openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.NORMAL, false);

        // Write less than threshold (5 < 10 bytes) — should NOT trigger fsync
        fs.writeSync(file, bufOf(new byte[5]));
        assertEquals(5, file.pendingFsyncBytes);

        // Write enough to exceed threshold (5 + 10 = 15 >= 10) — triggers fsync
        fs.writeSync(file, bufOf(new byte[10]));
        assertEquals(0, file.pendingFsyncBytes);

        StorageUtil.closeChannels(fs.closeSync(file));
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
        AsyncSegmentFile seg = openSeg(dir, true);
        // openInitialResources creates the tail segment metadata for writers on an empty dir,
        // and openWithFileEntry then opens its channel, so segment 0 exists both ways.
        assertEquals(Collections.singletonList(0L), listSeg(seg));
        assertEquals(0, seg.openedSegmentStartOffset);
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        StorageUtil.closeChannels(fs.closeSync(seg));
    }

    @Test
    public void testSegmentWriteAndRead() throws Exception {
        String dir = path("segdir2");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segW = openSeg(dir, true);
        byte[] data = new byte[]{1, 2, 3, 4, 5};
        writeSeg(segW, bufOf(data));
        StorageUtil.closeChannels(fs.closeSync(segW));

        AsyncSegmentFile segR = openSeg(dir, false);
        ByteBuf buf = fs.readSync(segR, 5, segR.position);
        try {
            byte[] actual = new byte[buf.readableBytes()];
            buf.readBytes(actual);
            assertArrayEquals(data, actual);
        } finally {
            buf.release();
        }
        StorageUtil.closeChannels(fs.closeSync(segR));
    }

    @Test
    public void testSegmentReadAcrossSegmentsNeedsReposition() throws Exception {
        String dir = path("segdir3");
        Files.createDirectories(Paths.get(dir));
        // Write two segments via roll: seg0 = [0, 3), seg3 = [3, 6)
        AsyncSegmentFile segW = openSeg(dir, true);
        writeSeg(segW, bufOf(new byte[]{10, 20, 30}));
        rollSeg(segW);
        writeSeg(segW, bufOf(new byte[]{40, 50, 60}));
        StorageUtil.closeChannels(fs.closeSync(segW));

        AsyncSegmentFile segR = openSeg(dir, false);
        assertEquals(Arrays.asList(0L, 3L), listSeg(segR));
        assertEquals(0, segR.openedSegmentStartOffset);
        assertEquals(3, segR.openedSegmentEndOffset);

        ByteBuf buf1 = fs.readSync(segR, 3, segR.position);
        try {
            byte[] d1 = new byte[3];
            buf1.readBytes(d1);
            assertArrayEquals(new byte[]{10, 20, 30}, d1);
        } finally {
            buf1.release();
        }
        // Crossing the boundary only drops the channel; it does not advance the opened range,
        // so readSync alone would re-open seg0 and read past its end.
        assertNull(segR.currentSegmentChannel);
        assertEquals(0, segR.openedSegmentStartOffset);
        ByteBuf stale = fs.readSync(segR, 3, 3);
        try {
            assertEquals(0, stale.readableBytes());
        } finally {
            stale.release();
        }

        // The caller must reposition: positionSync switches the opened range to seg3.
        positionSeg(segR, 3);
        assertEquals(3, segR.openedSegmentStartOffset);
        ByteBuf buf2 = fs.readSync(segR, 3, segR.position);
        try {
            byte[] d2 = new byte[3];
            buf2.readBytes(d2);
            assertArrayEquals(new byte[]{40, 50, 60}, d2);
        } finally {
            buf2.release();
        }
        StorageUtil.closeChannels(fs.closeSync(segR));
    }

    @Test
    public void testSegmentPread() throws Exception {
        String dir = path("segdir4");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segW = openSeg(dir, true);
        writeSeg(segW, bufOf(new byte[]{1, 2, 3, 4, 5}));
        StorageUtil.closeChannels(fs.closeSync(segW));

        AsyncSegmentFile segR = openSeg(dir, false);
        // pread at offset 2
        ByteBuf buf = fs.readSync(segR, 3, 2);
        try {
            byte[] data = new byte[3];
            buf.readBytes(data);
            assertArrayEquals(new byte[]{3, 4, 5}, data);
        } finally {
            buf.release();
        }
        StorageUtil.closeChannels(fs.closeSync(segR));
    }

    @Test
    public void testSegmentRoll() throws Exception {
        String dir = path("segdir5");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = openSeg(dir, true);
        writeSeg(seg, bufOf(new byte[]{1, 2, 3}));
        rollSeg(seg);
        List<Long> offsets = listSeg(seg);
        assertEquals(2, offsets.size());
        assertEquals(Long.valueOf(0), offsets.get(0));
        assertEquals(Long.valueOf(3), offsets.get(1));
        StorageUtil.closeChannels(fs.closeSync(seg));
    }

    @Test
    public void testSegmentRollEmptyIsNoOp() throws Exception {
        String dir = path("segdir6");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = openSeg(dir, true);
        // First roll creates segment at offset 0
        rollSeg(seg);
        // Second roll with empty segment is a no-op (no new segment created)
        List<Long> offsetsBefore = listSeg(seg);
        rollSeg(seg);
        List<Long> offsetsAfter = listSeg(seg);
        assertEquals(offsetsBefore, offsetsAfter);
        StorageUtil.closeChannels(fs.closeSync(seg));
    }

    @Test
    public void testSegmentList() throws Exception {
        String dir = path("segdir7");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = openSeg(dir, true);
        writeSeg(seg, bufOf(new byte[10]));
        rollSeg(seg);
        writeSeg(seg, bufOf(new byte[20]));
        rollSeg(seg);
        writeSeg(seg, bufOf(new byte[5]));
        List<Long> offsets = listSeg(seg);
        assertEquals(Arrays.asList(0L, 10L, 30L), offsets);
        StorageUtil.closeChannels(fs.closeSync(seg));
    }

    @Test
    public void testSegmentSizeSync() throws Exception {
        String dir = path("segdir8");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = openSeg(dir, true);
        writeSeg(seg, bufOf(new byte[10]));
        rollSeg(seg);
        writeSeg(seg, bufOf(new byte[20]));
        assertEquals(30, fs.sizeSync(seg));
        StorageUtil.closeChannels(fs.closeSync(seg));
    }

    @Test
    public void testSegmentTruncateInRange() throws Exception {
        String dir = path("segdir9");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = openSeg(dir, true);
        writeSeg(seg, bufOf(new byte[10]));
        rollSeg(seg);
        writeSeg(seg, bufOf(new byte[20]));
        // Truncate at offset 15 (inside second segment [10, 30))
        truncSeg(seg, 15);
        List<Long> offsets = listSeg(seg);
        assertEquals(Arrays.asList(0L, 10L), offsets);
        // Second segment should be truncated to 5 bytes
        assertEquals(5, fs.sizeOfSegmentSync(seg, 10));
        StorageUtil.closeChannels(fs.closeSync(seg));
    }

    @Test
    public void testSegmentTruncateOffsetBeforeFirst() throws Exception {
        String dir = path("segdir10");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = openSeg(dir, true);
        writeSeg(seg, bufOf(new byte[10]));
        rollSeg(seg);
        writeSeg(seg, bufOf(new byte[20]));
        // Truncate at offset -100 (before first segment at 0) -> resets everything
        truncSeg(seg, -100);
        // Should create a new empty segment at offset -100
        List<Long> offsets = listSeg(seg);
        assertEquals(1, offsets.size());
        assertEquals(Long.valueOf(-100), offsets.get(0));
        // Old segment files should be deleted
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "10")));
        StorageUtil.closeChannels(fs.closeSync(seg));
    }

    @Test
    public void testSegmentDeleteSegments() throws Exception {
        String dir = path("segdir12");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = openSeg(dir, true);
        writeSeg(seg, bufOf(new byte[10]));
        rollSeg(seg);
        writeSeg(seg, bufOf(new byte[20]));
        rollSeg(seg);
        writeSeg(seg, bufOf(new byte[5]));
        // Delete first segment (offset 0)
        deleteSegs(seg, Collections.singletonList(0L));
        List<Long> offsets = listSeg(seg);
        assertEquals(Arrays.asList(10L, 30L), offsets);
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "10")));
        StorageUtil.closeChannels(fs.closeSync(seg));
    }

    @Test
    public void testSegmentDeleteAll() throws Exception {
        String dir = path("segdir13");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = openSeg(dir, true);
        writeSeg(seg, bufOf(new byte[10]));
        rollSeg(seg);
        writeSeg(seg, bufOf(new byte[20]));
        deleteSeg(seg);
        // All segment and index files should be deleted
        File[] remaining = new File(dir).listFiles();
        if (remaining != null) {
            for (File f : remaining) {
                String name = f.getName();
                assertFalse("segment file should be deleted: " + name, name.startsWith(SEG_PREFIX));
                assertFalse("index file should be deleted: " + name, name.startsWith(IDX_PREFIX));
            }
        }
        StorageUtil.closeChannels(fs.closeSync(seg));
    }

    @Test
    public void testSegmentTransferToSync() throws Exception {
        String dir = path("segdir15");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segW = openSeg(dir, true);
        writeSeg(segW, bufOf(new byte[]{10, 20, 30, 40, 50}));
        StorageUtil.closeChannels(fs.closeSync(segW));

        AsyncSegmentFile segR = openSeg(dir, false);
        ByteArrayOutputStreamChannel target = new ByteArrayOutputStreamChannel();
        long n = fs.transferToSync(segR, 1, 3, target);
        assertEquals(3, n);
        assertArrayEquals(new byte[]{20, 30, 40}, target.toByteArray());
        StorageUtil.closeChannels(fs.closeSync(segR));
    }

    // =========================================================================
    // G. Segment open recovery & dirty data deletion
    // =========================================================================

    @Test
    public void testSegmentOpenDeletesOffChainSegments() throws Exception {
        String dir = path("segdir18");
        Files.createDirectories(Paths.get(dir));
        // Segment at offset 0, size 10 -> covers [0, 10)
        writeFile(dir + "/" + SEG_PREFIX + "0", new byte[10]);
        // Segment at offset 20, size 5 -> covers [20, 25) -- gap! not contiguous with [0, 10)
        writeFile(dir + "/" + SEG_PREFIX + "20", new byte[5]);

        // Only the highest segment chain is kept in the state.
        // The algorithm starts from highest offset and builds chain downward.
        // Highest is 20 (size 5, end=25). Next is 0 (size 10, end=10 != 20). So 0 is off-chain.
        AsyncSegmentFile reader = openSeg(dir, false);
        assertEquals(Collections.singletonList(20L), listSeg(reader));
        // A reader has no write permission, so the off-chain file is only logged, not deleted.
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));

        // The writer joins the entry the reader initialized and does the deletion.
        AsyncSegmentFile writer = openSeg(dir, true);
        assertEquals(Collections.singletonList(20L), listSeg(writer));
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "20")));
        StorageUtil.closeChannels(fs.closeSync(writer));
        StorageUtil.closeChannels(fs.closeSync(reader));
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

        // A reader keeps the overlapping file on disk, it is only excluded from the state.
        AsyncSegmentFile reader = openSeg(dir, false);
        assertEquals(Collections.singletonList(10L), listSeg(reader));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        StorageUtil.closeChannels(fs.closeSync(reader));

        // The entry is gone with the reader, so the writer inits from disk itself and, having write
        // permission, deletes the overlapping file during the init.
        AsyncSegmentFile writer = openSeg(dir, true);
        assertEquals(Collections.singletonList(10L), listSeg(writer));
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "10")));
        StorageUtil.closeChannels(fs.closeSync(writer));
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

        // A reader leaves the orphan index file alone.
        AsyncSegmentFile reader = openSeg(dir, false);
        assertTrue(Files.exists(Paths.get(dir, IDX_PREFIX + "100")));

        // The writer joins the entry and deletes it, keeping the index of the valid segment.
        AsyncSegmentFile writer = openSeg(dir, true);
        assertFalse(Files.exists(Paths.get(dir, IDX_PREFIX + "100")));
        assertTrue(Files.exists(Paths.get(dir, IDX_PREFIX + "0")));
        StorageUtil.closeChannels(fs.closeSync(writer));
        StorageUtil.closeChannels(fs.closeSync(reader));
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

        AsyncSegmentFile seg = openSeg(dir, true);
        // Valid chain should be preserved
        List<Long> offsets = listSeg(seg);
        assertEquals(Arrays.asList(0L, 10L, 30L), offsets);
        // Garbage should be deleted
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "BADNAME")));
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "BADNAME2")));
        assertFalse(Files.exists(Paths.get(dir, IDX_PREFIX + "888")));
        // Valid segment files should still exist
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "0")));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "10")));
        assertTrue(Files.exists(Paths.get(dir, SEG_PREFIX + "30")));
        StorageUtil.closeChannels(fs.closeSync(seg));
    }

    // =========================================================================
    // H. AsyncFile special branches
    // =========================================================================

    @Test
    public void testLenientOpenNonRegularFile() throws Exception {
        // lenient=true with a directory path → channel not opened, file object still created
        String dir = path("lenient_dir");
        Files.createDirectories(Paths.get(dir));
        AsyncFile file = openFile(dir, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.NORMAL, true);
        assertNotNull(file);
        // readSync should NPE because channel is null (lenient skipped openCurrentChannel)
        try {
            fs.readSync(file, 1, 0, 0);
            fail("Expected NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
        StorageUtil.closeChannels(fs.closeSync(file));
    }

    @Test
    public void testOpenReadWriteMode() throws Exception {
        // OpenMode.READ_WRITE: file opened for both read and write, positioned at end
        String p = path("rw_file");
        writeFile(p, new byte[]{1, 2, 3, 4, 5});
        AsyncFile file = openFile(p, AbstractStorageFile.OpenMode.READ_WRITE, AbstractStorageFile.ReplaceMode.NORMAL, false);
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
        StorageUtil.closeChannels(fs.closeSync(file));
    }

    @Test
    public void testAutoFsyncByTimeInterval() throws Exception {
        // Byte threshold very high so only the time threshold can trigger the fsync.
        fs.setFsyncIntervalBytes(Long.MAX_VALUE / 2);
        // lastFsyncNanos starts at file construction time, so a 1ms threshold can already be
        // exceeded by the first write on a loaded machine. Keep it out of reach until the
        // pendingFsyncBytes accumulation has been observed.
        fs.setFsyncIntervalMillis(Long.MAX_VALUE / 2_000_000L);
        String p = path("file_time_fsync");
        AsyncFile file = openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.NORMAL, false);
        // Write a few bytes — neither threshold reached
        fs.writeSync(file, bufOf(new byte[5]));
        assertEquals(5, file.pendingFsyncBytes);
        // Lower the threshold and sleep past it
        fs.setFsyncIntervalMillis(1);
        Thread.sleep(50);
        // Write a few more bytes — time threshold exceeded, triggers fsync
        fs.writeSync(file, bufOf(new byte[3]));
        assertEquals(0, file.pendingFsyncBytes);
        StorageUtil.closeChannels(fs.closeSync(file));
    }

    @Test
    public void testTruncateAtomicReplaceNoPositionChange() throws Exception {
        // An atomic replace truncate should NOT change channel position
        String p = path("file_trunc_ar");
        AsyncFile writer = openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.ATOMIC, false);
        fs.writeSync(writer, bufOf(new byte[100]));
        // atomicReplaceWrite sets position to 0 then writes, so position=100 after write
        // Truncate to 50 — the atomic replace path skips channel.position(size)
        fs.truncateSync(writer, 50);
        // Position should NOT have been changed to 50 by truncate
        // (for an atomic replace, position is left as-is after truncate)
        StorageUtil.closeChannels(fs.closeSync(writer));
        // File should be truncated to 50 bytes on disk
        assertEquals(50, Files.size(Paths.get(p)));
    }

    @Test
    public void testTruncateReducesPendingFsyncBytes() throws Exception {
        // truncateSync: pendingFsyncBytes reduction logic (line 351) runs,
        // then fsyncInternal at line 353 resets to 0. Verify end-to-end correctness.
        String p = path("file_trunc_pending");
        AsyncFile writer = openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.NORMAL, false);
        fs.writeSync(writer, bufOf(new byte[100]));
        assertEquals(100, writer.pendingFsyncBytes);
        // truncateSync reduces pendingFsyncBytes by (100-30)=70, then fsyncInternal resets to 0
        fs.truncateSync(writer, 30);
        assertEquals(0, writer.pendingFsyncBytes); // reset by fsyncInternal
        assertEquals(30, fs.sizeSync(writer));
        // Write more after truncate — pendingFsyncBytes should accumulate normally
        fs.writeSync(writer, bufOf(new byte[20]));
        assertEquals(20, writer.pendingFsyncBytes);
        StorageUtil.closeChannels(fs.closeSync(writer));
    }

    @Test
    public void testCloseSyncIdempotent() throws Exception {
        String p = path("file_double_close");
        AsyncFile file = openFile(p, AbstractStorageFile.OpenMode.WRITE, AbstractStorageFile.ReplaceMode.NORMAL, false);
        fs.writeSync(file, bufOf(new byte[]{1}));
        StorageUtil.closeChannels(fs.closeSync(file));
        // Second close should be a no-op, not throw
        StorageUtil.closeChannels(fs.closeSync(file));
    }

    // =========================================================================
    // I. Segment special branches
    // =========================================================================

    @Test
    public void testSegmentTruncateAcrossSegments() throws Exception {
        // Truncate to offset inside a different segment than currently opened by writer
        String dir = path("seg_trunc_cross");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = openSeg(dir, true);
        writeSeg(seg, bufOf(new byte[10])); // segment [0, 10)
        rollSeg(seg);
        writeSeg(seg, bufOf(new byte[10])); // segment [10, 20)
        // Writer currently has segment [10, 20) opened
        assertEquals(10, seg.openedSegmentStartOffset);
        // Truncate at offset 5 (inside segment [0, 10)) — needs to close [10,20), open [0,10)
        truncSeg(seg, 5);
        // Writer should have switched to segment [0, 10)
        assertEquals(0, seg.openedSegmentStartOffset);
        // Segment [10, 20) should be deleted
        assertFalse(Files.exists(Paths.get(dir, SEG_PREFIX + "10")));
        // Segment 0 should be truncated to 5 bytes
        assertEquals(5, fs.sizeOfSegmentSync(seg, 0));
        // Index file for segment 0 should exist
        assertTrue(Files.exists(Paths.get(dir, IDX_PREFIX + "0")));
        List<Long> offsets = listSeg(seg);
        assertEquals(Collections.singletonList(0L), offsets);
        // After truncate, position is still 20; getCurrentSegmentStartOffset uses floorKey(20)=0 (only one segment)
        assertEquals(0, fs.getCurrentSegmentStartOffset(seg));
        // After repositioning to the truncated segment, start offset should be 0
        positionSeg(seg, 3);
        assertEquals(0, fs.getCurrentSegmentStartOffset(seg));
        StorageUtil.closeChannels(fs.closeSync(seg));
    }

    @Test
    public void testGetCurrentIndexFilesSyncWriterUsesSegmentCreatedAtOpen() throws Exception {
        // Writer's tail segment is created at open, so no roll happens here.
        String dir = path("seg_idx_empty_w");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = openSeg(dir, true);
        assertEquals(Collections.singletonList(0L), listSeg(seg));
        Pair<Long, Map<String, AsyncIndexFile>> result = fs.getCurrentIndexFilesSync(seg, INDEX_PREFIXES, false);
        assertEquals(Long.valueOf(0), result.getKey());
        assertTrue(result.getValue().containsKey(IDX_PREFIX));
        assertEquals(1, listSeg(seg).size());
        StorageUtil.closeChannels(fs.closeSync(seg));
    }

    @Test
    public void testGetStartOffsetByReadOffset() throws Exception {
        String dir = path("seg_start_off");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segW = openSeg(dir, true);
        writeSeg(segW, bufOf(new byte[10]));  // segment [0, 10)
        rollSeg(segW);
        writeSeg(segW, bufOf(new byte[20]));  // segment [10, 30)
        rollSeg(segW);
        writeSeg(segW, bufOf(new byte[5]));   // segment [30, 35)
        StorageUtil.closeChannels(fs.closeSync(segW));

        AsyncSegmentFile segR = openSeg(dir, false);
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
        StorageUtil.closeChannels(fs.closeSync(segR));
    }

    @Test
    public void testGetCurrentSegmentStartOffsetWriter() throws Exception {
        // Writer always returns the currently opened segment start offset
        String dir = path("seg_cur_off_w");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = openSeg(dir, true);
        // Empty state: openedSegmentStartOffset=0 (markEmptyOpenedRange)
        assertEquals(0, fs.getCurrentSegmentStartOffset(seg));
        writeSeg(seg, bufOf(new byte[10]));
        // Still on segment 0
        assertEquals(0, fs.getCurrentSegmentStartOffset(seg));
        rollSeg(seg);
        // Now on segment 10
        assertEquals(10, fs.getCurrentSegmentStartOffset(seg));
        writeSeg(seg, bufOf(new byte[5]));
        assertEquals(10, fs.getCurrentSegmentStartOffset(seg));
        StorageUtil.closeChannels(fs.closeSync(seg));
    }

    @Test
    public void testGetCurrentSegmentStartOffsetReader() throws Exception {
        // Reader uses floorKey(position) to find segment start offset
        String dir = path("seg_cur_off_r");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segW = openSeg(dir, true);
        writeSeg(segW, bufOf(new byte[10]));  // [0, 10)
        rollSeg(segW);
        writeSeg(segW, bufOf(new byte[20]));  // [10, 30)
        StorageUtil.closeChannels(fs.closeSync(segW));

        AsyncSegmentFile segR = openSeg(dir, false);
        // Reader position=0 (firstOffset) → segment 0
        assertEquals(0, fs.getCurrentSegmentStartOffset(segR));
        // Move position into second segment
        positionSeg(segR, 15);
        assertEquals(10, fs.getCurrentSegmentStartOffset(segR));
        // Move position to exact boundary
        positionSeg(segR, 10);
        assertEquals(10, fs.getCurrentSegmentStartOffset(segR));
        StorageUtil.closeChannels(fs.closeSync(segR));
    }

    @Test
    public void testSegmentPositionSyncCloseCurrentRecalc() throws Exception {
        // When shared state changes (new segments added) and openedSegmentEndOffset=MAX_VALUE
        // for a non-last segment, positionSync triggers closeCurrent for recalculation.
        String dir = path("seg_pos_recalc");
        Files.createDirectories(Paths.get(dir));

        // Writer1 creates segments [0,10) and [10,20)
        AsyncSegmentFile segWriter1 = openSeg(dir, true);
        writeSeg(segWriter1, bufOf(new byte[10]));
        rollSeg(segWriter1);
        writeSeg(segWriter1, bufOf(new byte[10]));
        StorageUtil.closeChannels(fs.closeSync(segWriter1));

        // Reader opens — shares FileEntry with segWriter1
        AsyncSegmentFile segReader = openSeg(dir, false);
        // openInitialResources: state=[0,10],[10,20], sets position=0, switchToSegment(0,s)
        // → openedSegmentStartOffset=0, openedSegmentEndOffset=10

        // Writer2 (same key) adds segment [20,30), updating shared state to [0,10],[10,20],[20,30]
        AsyncSegmentFile segWriter2 = openSeg(dir, true);
        writeSeg(segWriter2, bufOf(new byte[10]));
        StorageUtil.closeChannels(fs.closeSync(segWriter2));

        // positionSync: openedSegmentEndOffset=10 (not MAX_VALUE)
        // → closeCurrent condition is NOT met for this case.
        // We need a scenario where openedSegmentEndOffset=MAX_VALUE for non-last segment.
        // That happens when reader was opened with empty state (markEmptyOpenedRange → MAX_VALUE).

        // Clean up for a fresh scenario
        StorageUtil.closeChannels(fs.closeSync(segReader));

        // Fresh scenario: writer opens empty dir (empty state → MAX_VALUE end)
        String dir2 = path("seg_pos_recalc2");
        Files.createDirectories(Paths.get(dir2));
        AsyncSegmentFile segW3 = openSeg(dir2, true);
        // State is empty, openedSegmentEndOffset = Long.MAX_VALUE
        StorageUtil.closeChannels(fs.closeSync(segW3));

        // Reader opens — state is empty, markEmptyOpenedRange → MAX_VALUE
        AsyncSegmentFile segR2 = openSeg(dir2, false);
        assertEquals(Long.MAX_VALUE, segR2.openedSegmentEndOffset);

        // Writer adds segments, updating shared state
        AsyncSegmentFile segW4 = openSeg(dir2, true);
        writeSeg(segW4, bufOf(new byte[10]));
        rollSeg(segW4);
        writeSeg(segW4, bufOf(new byte[10]));
        StorageUtil.closeChannels(fs.closeSync(segW4));

        // Now shared state is [0,10],[10,20]
        // positionSync: openedSegmentEndOffset=MAX_VALUE, state not empty,
        // openedSegmentStartOffset(0) != s.lastOffset(10) → triggers closeCurrent!
        positionSeg(segR2, 5);
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
        StorageUtil.closeChannels(fs.closeSync(segR2));
    }

    @Test
    public void testMaybeSwitchSegmentStaleTailEof() throws Exception {
        // When segment is shrunk externally and reader has stale end offset,
        // maybeSwitchSegment detects staleTailEof and recalculates from metadata
        // (opened range uses nextStart; no Files.size continuity check).
        String dir = path("seg_stale_eof");
        Files.createDirectories(Paths.get(dir));

        // Create 2 segments: [0,10) and [10,20)
        AsyncSegmentFile segW = openSeg(dir, true);
        writeSeg(segW, bufOf(new byte[10]));
        rollSeg(segW);
        writeSeg(segW, bufOf(new byte[10]));
        StorageUtil.closeChannels(fs.closeSync(segW));

        // Reader opens — state=[0,10],[10,20], position=0
        // switchToSegment(0) → openedSegmentStartOffset=0, openedSegmentEndOffset=10
        AsyncSegmentFile segR = openSeg(dir, false);
        assertEquals(0, segR.openedSegmentStartOffset);
        assertEquals(10, segR.openedSegmentEndOffset);

        // Shrink segment 0 to 3 bytes externally
        Path segPath = Paths.get(dir, SEG_PREFIX + "0");
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(segPath.toFile(), "rw")) {
            raf.setLength(3);
        }

        // Read at offset 5 (past shrunk file size of 3) → 0 bytes, then staleTailEof
        // recalculates opened range from metadata still as [0, 10).
        ByteBuf buf = fs.readSync(segR, 5, 5);
        try {
            assertEquals(0, buf.readableBytes());
        } finally {
            buf.release();
        }
        assertEquals(0, segR.openedSegmentStartOffset);
        assertEquals(10, segR.openedSegmentEndOffset);
        StorageUtil.closeChannels(fs.closeSync(segR));
    }

    @Test
    public void testReadSyncEmptyStateThrowsStaleState() throws Exception {
        // readSync assumes the caller already switched to a segment covering the offset, so it
        // has no empty-state guard: it opens segmentPath(openedSegmentStartOffset) and the
        // missing file surfaces as StaleStateException.
        // The empty-buffer answer belongs to TailCacheFileSystem, whose preReadMetadata gets
        // false back from switchToSegment on empty state and skips the read entirely.
        String dir = path("seg_read_empty");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = openSeg(dir, false);
        assertTrue(listSeg(seg).isEmpty());
        // positionSync on empty state marks the opened range empty without opening a channel.
        positionSeg(seg, 0);
        assertNull(seg.currentSegmentChannel);
        try {
            fs.readSync(seg, 10, 0);
            fail("Expected StaleStateException for read on empty segment state");
        } catch (StaleStateException e) {
            assertTrue(e.getCause() instanceof java.nio.file.NoSuchFileException);
        }
        StorageUtil.closeChannels(fs.closeSync(seg));
    }

    @Test
    public void testReaderGetCurrentIndexFilesOpensFromDisk() throws Exception {
        // Reader's getCurrentIndexFiles lazily opens index files from disk
        // when they're not in the current cache (cleared by switchToSegment)
        String dir = path("seg_lazy_idx");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segW = openSeg(dir, true);
        writeSeg(segW, bufOf(new byte[10])); // segment 0 with idx0
        rollSeg(segW);
        writeSeg(segW, bufOf(new byte[20])); // segment 10 with idx10
        StorageUtil.closeChannels(fs.closeSync(segW));

        // Verify index files exist on disk
        assertTrue(Files.exists(Paths.get(dir, IDX_PREFIX + "0")));
        assertTrue(Files.exists(Paths.get(dir, IDX_PREFIX + "10")));

        // Reader opens — openInitialResources positions at 0, switchToSegment(0)
        // which clears currentIndexFiles
        AsyncSegmentFile segR = openSeg(dir, false);
        // getCurrentIndexFilesSync → switchToSegment(0) again (clears cache again)
        // → getCurrentIndexFiles finds idx not in cache, opens from disk
        Pair<Long, Map<String, AsyncIndexFile>> result = fs.getCurrentIndexFilesSync(segR, INDEX_PREFIXES, false);
        assertEquals(Long.valueOf(0), result.getKey());
        assertTrue(result.getValue().containsKey(IDX_PREFIX));
        // Verify the returned index file is functional
        AsyncFile idxFile = result.getValue().get(IDX_PREFIX);
        assertNotNull(idxFile);
        StorageUtil.closeChannels(fs.closeSync(segR));
    }

    // =========================================================================
    // J. Metadata/directory special branches
    // =========================================================================

    @Test
    public void testLastModifiedAsyncFile() throws Exception {
        String p = path("file_lm");
        writeFile(p, new byte[]{1, 2, 3});
        AsyncFile file = openFile(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.NORMAL, false);
        long lm = fs.lastModified(file).get();
        assertTrue("lastModified should be positive", lm > 0);
        StorageUtil.closeChannels(fs.closeSync(file));
    }

    @Test
    public void testLastModifiedSegmentEmpty() throws Exception {
        String dir = path("seg_lm_empty");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = openSeg(dir, false);
        long lm = fs.lastModified(seg).get();
        assertEquals(0L, lm);
        StorageUtil.closeChannels(fs.closeSync(seg));
    }

    @Test
    public void testLastModifiedSegmentNonEmpty() throws Exception {
        String dir = path("seg_lm");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile segW = openSeg(dir, true);
        writeSeg(segW, bufOf(new byte[10]));
        StorageUtil.closeChannels(fs.closeSync(segW));

        AsyncSegmentFile segR = openSeg(dir, false);
        long lm = fs.lastModified(segR).get();
        assertTrue("lastModified should be positive for non-empty segment", lm > 0);
        StorageUtil.closeChannels(fs.closeSync(segR));
    }

    @Test
    public void testLastModifiedOfSegmentMissing() throws Exception {
        String dir = path("seg_lm_miss");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = openSeg(dir, true);
        writeSeg(seg, bufOf(new byte[10]));
        StorageUtil.closeChannels(fs.closeSync(seg));

        // Reopen and delete segment file externally
        AsyncSegmentFile segR = openSeg(dir, false);
        Files.delete(Paths.get(dir, SEG_PREFIX + "0"));
        long lm = fs.lastModifiedOfSegment(segR, 0).get();
        assertEquals(0L, lm);
        StorageUtil.closeChannels(fs.closeSync(segR));
    }

    @Test
    public void testSizeOfSegmentSyncExisting() throws Exception {
        String dir = path("seg_size_exist");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = openSeg(dir, true);
        writeSeg(seg, bufOf(new byte[42]));
        StorageUtil.closeChannels(fs.closeSync(seg));

        AsyncSegmentFile segR = openSeg(dir, false);
        assertEquals(42, fs.sizeOfSegmentSync(segR, 0));
        StorageUtil.closeChannels(fs.closeSync(segR));
    }

    @Test
    public void testSizeOfSegmentSyncMissing() throws Exception {
        String dir = path("seg_size_miss");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile seg = openSeg(dir, false);
        assertEquals(0L, fs.sizeOfSegmentSync(seg, 999));
        StorageUtil.closeChannels(fs.closeSync(seg));
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
        // list() no longer swallows the IO error into an empty result:
        // Files.list on a regular file throws NotDirectoryException, which
        // wrapIOException maps to IllegalArgumentException.
        String p = path("a_file");
        writeFile(p, new byte[]{1});
        try {
            fs.list(p).get();
            fail("Expected IllegalArgumentException for list on a regular file");
        } catch (java.util.concurrent.ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
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
