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
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

import static org.junit.Assert.*;

/** keeper-tfs-m6 §17.3: every channel of the backing FS is opened through its FileChannelFactory. */
public class AsyncTFSBasedFileSystemChannelFactoryTest {

    private static final BiConsumer<String, CompletableFuture<?>> NO_REGISTER = (key, future) -> { };
    private static final BiConsumer<String, List<FileChannel>> CLOSE_CHANNELS =
            (path, channels) -> StorageUtil.closeChannels(channels);
    private static final long TIMEOUT_MS = 5_000;
    private static final String SEG_PREFIX = "seg";
    private static final String IDX_PREFIX = "idx";
    private static final String TMP_PREFIX = "TMP_REP_";

    private ExecutorService ioExecutor;
    private RecordingFactory factory;
    private AsyncTFSBasedFileSystem fs;
    private Path tempDir;
    private final List<AbstractStorageFile> opened = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("tfs-factory-test-");
        ioExecutor = Executors.newCachedThreadPool();
        factory = new RecordingFactory();
        fs = new AsyncTFSBasedFileSystem(ioExecutor, Long.MAX_VALUE / 2, Long.MAX_VALUE / 2_000_000L, factory);
    }

    @After
    public void tearDown() {
        for (AbstractStorageFile file : opened) {
            if (file instanceof AsyncSegmentFile) {
                StorageUtil.closeChannels(fs.closeSync((AsyncSegmentFile) file));
            } else {
                StorageUtil.closeChannels(fs.closeSync((AsyncFile) file));
            }
        }
        ioExecutor.shutdownNow();
        deleteRecursively(tempDir.toFile());
    }

    @Test(expected = NullPointerException.class)
    public void testNullFactoryRejected() {
        new AsyncTFSBasedFileSystem(ioExecutor, 1, 1, null);
    }

    @Test
    public void testDefaultConstructorOpensRawChannel() {
        fs = new AsyncTFSBasedFileSystem(ioExecutor, Long.MAX_VALUE / 2, Long.MAX_VALUE / 2_000_000L);
        AsyncFile file = openFile(path("raw"), AbstractStorageFile.OpenMode.READ_WRITE,
                AbstractStorageFile.ReplaceMode.NORMAL);
        assertSame(FileChannelFactory.DEFAULT, file.channelFactory);
        assertTrue(file.channel.getClass().getName(), file.channel.getClass().getName().startsWith("sun.nio.ch."));
        assertTrue(factory.calls.isEmpty());
    }

    @Test
    public void testAsyncFileWriteAndReadOpens() throws Exception {
        String p = path("file");
        AsyncFile writer = openFile(p, AbstractStorageFile.OpenMode.READ_WRITE, AbstractStorageFile.ReplaceMode.NORMAL);
        assertSame(factory, writer.channelFactory);
        assertSame(factory.lastChannel(), writer.channel);
        factory.assertOpened("file", StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        fs.writeSync(writer, Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));
        factory.calls.clear();
        AsyncFile reader = openFile(p, AbstractStorageFile.OpenMode.READ, AbstractStorageFile.ReplaceMode.NORMAL);
        assertSame(factory.lastChannel(), reader.channel);
        factory.assertOpened("file", StandardOpenOption.READ);
        assertArrayEquals(new byte[]{1, 2, 3}, read(fs.readSync(reader, 3, 0, 0)));
    }

    @Test
    public void testSegmentAndIndexOpens() throws Exception {
        String dir = path("segdir");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = openSeg(dir, true);
        assertSame(factory, writer.channelFactory);
        assertSame(factory.lastChannelOf(SEG_PREFIX + "0"), writer.currentSegmentChannel);
        factory.assertOpened(SEG_PREFIX + "0", StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        factory.assertOpened(IDX_PREFIX + "0", StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
        for (AsyncIndexFile index : writer.currentIndexFiles.values()) {
            assertSame(factory, index.channelFactory);
        }

        fs.writeSync(writer, Unpooled.wrappedBuffer(new byte[]{4, 5}));
        factory.calls.clear();
        AsyncSegmentFile reader = openSeg(dir, false);
        assertArrayEquals(new byte[]{4, 5}, read(fs.readSync(reader, 2, 0)));
        assertSame(factory.lastChannelOf(SEG_PREFIX + "0"), reader.currentSegmentChannel);
        factory.assertOpened(SEG_PREFIX + "0", StandardOpenOption.READ);
    }

    @Test
    public void testRewriteSegmentRangeOpens() throws Exception {
        String dir = path("rewritedir");
        Files.createDirectories(Paths.get(dir));
        AsyncSegmentFile writer = openSeg(dir, true);
        fs.writeSync(writer, Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));
        factory.calls.clear();

        assertTrue(fs.rewriteSegmentRangeSync(writer, 0, 1, from -> Unpooled.wrappedBuffer(new byte[]{9, 9})));
        factory.assertOpened(SEG_PREFIX + "0", StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                StandardOpenOption.READ);
        assertArrayEquals(new byte[]{1, 9, 9}, Files.readAllBytes(Paths.get(dir, SEG_PREFIX + "0")));
    }

    @Test
    public void testAtomicReplaceWriteOpensTmp() throws Exception {
        String p = path("atomic");
        AsyncFile writer = openFile(p, AbstractStorageFile.OpenMode.READ_WRITE, AbstractStorageFile.ReplaceMode.ATOMIC);
        factory.calls.clear();

        fs.writeSync(writer, Unpooled.wrappedBuffer(new byte[]{7, 8}));
        factory.assertOpened(TMP_PREFIX + "atomic", StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
        assertArrayEquals(new byte[]{7, 8}, Files.readAllBytes(Paths.get(p)));
    }

    @Test
    public void testTmpRecoveryOpens() throws Exception {
        String p = path("recover");
        Files.write(Paths.get(p), new byte[]{1});
        writeTmpFile(p, new byte[]{5, 6, 7});

        openFile(p, AbstractStorageFile.OpenMode.READ_WRITE, AbstractStorageFile.ReplaceMode.ATOMIC);
        factory.assertOpened(TMP_PREFIX + "recover", StandardOpenOption.READ);
        factory.assertOpened("recover", StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        assertArrayEquals(new byte[]{5, 6, 7}, Files.readAllBytes(Paths.get(p)));
    }

    @Test
    public void testPreferTmpReadOpensTmp() throws Exception {
        String p = path("prefer");
        Files.write(Paths.get(p), new byte[]{1});
        writeTmpFile(p, new byte[]{3, 4});
        AsyncFile reader = openFile(p, AbstractStorageFile.OpenMode.READ,
                AbstractStorageFile.ReplaceMode.ATOMIC_PREFER_TMP);
        factory.calls.clear();

        assertArrayEquals(new byte[]{3, 4}, read(fs.readSync(reader, 2, 0, 0)));
        factory.assertOpened(TMP_PREFIX + "prefer", StandardOpenOption.READ);
    }

    // ---- helpers ----

    private AsyncFile openFile(String filePath, AbstractStorageFile.OpenMode openMode,
            AbstractStorageFile.ReplaceMode replaceMode) {
        String key = StorageUtil.asyncFileKey(filePath);
        AsyncFile file = fs.openSync(filePath, key, key, openMode, replaceMode, false, null, false);
        fs.openWithFileEntry(file, false, NO_REGISTER, CLOSE_CHANNELS, TIMEOUT_MS, TIMEOUT_MS);
        opened.add(file);
        return file;
    }

    private AsyncSegmentFile openSeg(String dirPath, boolean write) {
        String key = StorageUtil.segmentKey(dirPath, SEG_PREFIX);
        AsyncSegmentFile seg = fs.openSync(dirPath, SEG_PREFIX, key, key, Collections.singletonList(IDX_PREFIX),
                write, null, false);
        fs.openWithFileEntry(seg, false, NO_REGISTER, CLOSE_CHANNELS, TIMEOUT_MS, TIMEOUT_MS);
        opened.add(seg);
        return seg;
    }

    // [8-byte length][data], the layout the atomic replace recovery expects.
    private static void writeTmpFile(String filePath, byte[] data) throws IOException {
        Path target = Paths.get(filePath);
        Path tmpPath = target.resolveSibling(TMP_PREFIX + target.getFileName());
        try (FileChannel ch = FileChannel.open(tmpPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.allocate(8 + data.length);
            buf.putLong(data.length).put(data).flip();
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
        }
    }

    private static byte[] read(ByteBuf buf) {
        try {
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            return bytes;
        } finally {
            buf.release();
        }
    }

    private String path(String name) {
        return tempDir.resolve(name).toString();
    }

    private static void deleteRecursively(File f) {
        File[] children = f.listFiles();
        if (children != null) {
            for (File c : children) {
                deleteRecursively(c);
            }
        }
        f.delete();
    }

    private static final class OpenCall {
        final String fileName;
        final Set<OpenOption> options;
        final FileChannel channel;

        OpenCall(String fileName, Set<OpenOption> options, FileChannel channel) {
            this.fileName = fileName;
            this.options = options;
            this.channel = channel;
        }

        @Override
        public String toString() {
            return fileName + options;
        }
    }

    private static final class RecordingFactory implements FileChannelFactory {
        final List<OpenCall> calls = new CopyOnWriteArrayList<>();

        @Override
        public FileChannel open(Path path, Set<? extends OpenOption> options) throws IOException {
            FileChannel channel = FileChannel.open(path, options);
            calls.add(new OpenCall(path.getFileName().toString(), new java.util.HashSet<>(options), channel));
            return channel;
        }

        FileChannel lastChannel() {
            return calls.get(calls.size() - 1).channel;
        }

        FileChannel lastChannelOf(String fileName) {
            FileChannel last = null;
            for (OpenCall call : calls) {
                if (call.fileName.equals(fileName)) {
                    last = call.channel;
                }
            }
            return last;
        }

        void assertOpened(String fileName, StandardOpenOption first, StandardOpenOption... rest) {
            Set<OpenOption> expected = new java.util.HashSet<>(EnumSet.of(first, rest));
            for (OpenCall call : calls) {
                if (call.fileName.equals(fileName) && call.options.equals(expected)) {
                    return;
                }
            }
            fail("no open of " + fileName + expected + " in " + calls);
        }
    }
}
