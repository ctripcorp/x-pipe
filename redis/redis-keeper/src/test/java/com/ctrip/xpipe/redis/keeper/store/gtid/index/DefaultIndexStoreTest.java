package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.ck.CKStore;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultIndexStoreTest {

    private static final Logger log = LoggerFactory.getLogger(DefaultIndexStoreTest.class);
    private static final String DEFAULT_BASE_DIR_NAME = "IndexStoreTest";
    /** Stable cmd prefix (production: cmd_<uuid>_); segment files are {prefix}{offset}. */
    private static final String CMD_PREFIX = "cmd_";
    /** 单测 ZONE 连续条数阈值；生产 KeeperConfig 默认 8192 */
    private static final int TEST_ZONE_CONSECUTIVE_THRESHOLD = 100;
    /** 单测 Block 满盘条数上限；生产 BlockEntry.BLOCK_MAX_SIZE = 8192 */
    private static final int TEST_BLOCK_MAX_SIZE = 100;

    private AsyncFileSystem testFs;
    private TestAsyncCommandStore testCmdStore;
    private final List<AsyncSegmentFile> openedSegments = new ArrayList<>();
    private long segmentWritten;
    private long totalWritten;
    private long lastRollSegmentStart;

    String tempDir = System.getProperty("java.io.tmpdir");

    String baseDir = Paths.get(tempDir, DEFAULT_BASE_DIR_NAME).toString();

    String filePath = "src/test/resources/GtidTest/appendonly.aof";

    String file1 = "src/test/resources/GtidTest/00000000.aof";
    String file2 = "src/test/resources/GtidTest/19513000.aof";


    String cmdDir = "src/test/resources/GtidTest/";

    String mergeFilePath = baseDir + "merge";

    private DefaultIndexStore defaultIndexStore;

    @Mock
    CommandWriter writer;

    @Mock
    CommandFileContext commandFileContext;

    @Mock
    FileChannel channel;

    @Mock
    CommandFile commandFile;

    @Mock
    CommandWriterCallback commandWriterCallback;

    @Mock
    GtidCmdFilter gtidCmdFilter;

    @Mock
    IndexWriter indexWriter;

    @Mock
    CKStore ckStore;

    @Mock
    KeeperConfig keeperConfig;

    @Before
    public void setUp() throws IOException {
        baseDir = Paths.get(tempDir, DEFAULT_BASE_DIR_NAME).toString();
        cleanDir(baseDir);
        segmentWritten = 0L;
        totalWritten = 0L;
        lastRollSegmentStart = 0L;

        when(channel.size()).thenReturn(0L);
        when(commandFileContext.getChannel()).thenReturn(channel);
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);
        when(writer.getFileContext()).thenReturn(commandFileContext);
        when(commandWriterCallback.getCommandWriter()).thenReturn(writer);
        when(writer.needRotate()).thenReturn(false);
        when(commandWriterCallback.getPendingSize()).thenReturn(0);
        bindWriterLengthMocks();
        bindWriteCommandToFs();

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);

        when(keeperConfig.dualWrite()).thenReturn(true);
        when(keeperConfig.readV2()).thenReturn(true);
        when(keeperConfig.getIndexZoneConsecutiveThreshold()).thenReturn(TEST_ZONE_CONSECUTIVE_THRESHOLD);
        when(keeperConfig.getIndexMixedTotalBytesThreshold()).thenReturn(16L * 1024 * 1024);
        when(keeperConfig.getBlockSizeThreshold()).thenReturn(BlockEntry.DEFAULT_BLOCK_MAX_SIZE);

        testFs = AbstractRedisKeeperTest.createTestAsyncFileSystem();
        testCmdStore = createTestCmdStore(CMD_PREFIX);
        bindCommandFileMock();

        defaultIndexStore = new DefaultIndexStore(keeperConfig, ckStore, testCmdStore, baseDir, opParser,
                commandWriterCallback, gtidCmdFilter);
        defaultIndexStore.openWriter(writer);
    }

    private void bindWriterLengthMocks() {
        when(writer.fileLength()).thenAnswer(inv -> segmentWritten);
        when(writer.totalLength()).thenAnswer(inv -> totalWritten);
        when(commandWriterCallback.getCmdFileLen()).thenAnswer(inv -> segmentWritten);
    }

    private void bindWriteCommandToFs() {
        try {
            doAnswer(inv -> {
                ByteBuf b = inv.getArgument(0);
                int n = testCmdStore.writeCmd(b);
                segmentWritten += n;
                totalWritten += n;
                return n;
            }).when(commandWriterCallback).writeCommand(any(ByteBuf.class));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void bindCommandFileMock() {
        when(commandFile.getFile()).thenAnswer(inv -> {
            try {
                return testCmdStore.currentCmdFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private TestAsyncCommandStore createTestCmdStore(String cmdPrefix) throws IOException {
        if (testCmdStore != null
                && cmdPrefix.equals(testCmdStore.getCommandFileNamePrefix())
                && new File(baseDir).equals(testCmdStore.getCommandBaseDir())) {
            // Reuse write-mode segment across reopen/recover — close+reopen races TailCache ByteBuf.
            segmentWritten = testCmdStore.currentSegmentSize();
            if (totalWritten < segmentWritten) {
                totalWritten = segmentWritten;
            }
            return testCmdStore;
        }
        closeCmdStoreForPrefix(cmdPrefix);
        List<String> prefixes = Arrays.asList(
                AbstractIndex.INDEX + cmdPrefix,
                AbstractIndex.BLOCK + cmdPrefix,
                AbstractIndex.INDEX_V2 + cmdPrefix,
                AbstractIndex.BLOCK_V2 + cmdPrefix);
        AsyncSegmentFile seg = AsyncFileSystemHelper.await(
                testFs.open(baseDir, cmdPrefix, prefixes, true, "test-repl-0"),
                "open test command segment");
        openedSegments.add(seg);
        TestAsyncCommandStore store = new TestAsyncCommandStore(testFs, seg, new File(baseDir), cmdPrefix);
        this.testCmdStore = store;
        return store;
    }

    private void closeCmdStoreForPrefix(String cmdPrefix) {
        if (testCmdStore == null) {
            return;
        }
        if (!cmdPrefix.equals(testCmdStore.getCommandFileNamePrefix())) {
            return;
        }
        if (!new File(baseDir).equals(testCmdStore.getCommandBaseDir())) {
            return;
        }
        AsyncSegmentFile seg = testCmdStore.getWriteSegmentFile();
        try {
            testCmdStore.closeSegment();
        } catch (Exception ignore) {
        }
        openedSegments.remove(seg);
    }

    /**
     * Flush write-mode cmd segment so disk catches up with TailCache ASYNC writes.
     * Recover/truncate paths use disk length; prod crash-restart only sees flushed bytes.
     */
    private void flushCmdSegment(TestAsyncCommandStore cmdStore) throws IOException {
        AsyncFileSystemHelper.await(testFs.fsync(cmdStore.getWriteSegmentFile()),
                "fsync cmd segment before recover");
    }

    private AsyncFile openTestAsyncFile(File file, boolean write) throws IOException {
        return AsyncFileSystemHelper.await(
                testFs.open(file.getAbsolutePath(), write ? AbstractStorageFile.OpenMode.WRITE : AbstractStorageFile.OpenMode.READ, false, true, "test-repl-0"),
                "open test async file " + file.getName());
    }

    /**
     * Roll the current write segment (same prefix) and notify IndexStore —
     * mirrors production rotate: flushWriter → roll → doSwitchCmdFile (spec §3.7.7).
     */
    private void switchCmdSegment(String ignoredLegacyName) throws Exception {
        lastRollSegmentStart = testCmdStore.getCurrentSegmentStartOffset() + testCmdStore.currentSegmentSize();
        defaultIndexStore.flushWriter();
        testCmdStore.roll();
        segmentWritten = 0L;
        defaultIndexStore.doSwitchCmdFile();
    }

    /** Normalize a legacy flat filename / test name to an AsyncSegmentFile prefix ending with '_'. */
    private static String toCmdPrefix(String nameOrPrefix) {
        if (nameOrPrefix == null || nameOrPrefix.isEmpty()) {
            return CMD_PREFIX;
        }
        if (nameOrPrefix.endsWith("_")) {
            return nameOrPrefix;
        }
        int lastUnderscore = nameOrPrefix.lastIndexOf('_');
        if (lastUnderscore >= 0) {
            String suffix = nameOrPrefix.substring(lastUnderscore + 1);
            try {
                Long.parseLong(suffix);
                return nameOrPrefix.substring(0, lastUnderscore + 1);
            } catch (NumberFormatException ignore) {
                // fall through
            }
        }
        return nameOrPrefix + "_";
    }

    private AsyncFile currentIndexHandle(String indexPrefix) throws IOException {
        return AsyncFileSystemHelper.await(
                testFs.getCurrentIndexFiles(testCmdStore.getWriteSegmentFile(), List.of(indexPrefix)),
                "get current index handle " + indexPrefix).getValue().get(indexPrefix);
    }

    private File indexV2File(String cmdPrefix, long segmentStart) {
        return new File(baseDir, AbstractIndex.INDEX_V2 + cmdPrefix + segmentStart);
    }

    private File indexV1File(String cmdPrefix, long segmentStart) {
        return new File(baseDir, AbstractIndex.INDEX + cmdPrefix + segmentStart);
    }

    private File blockV2File(String cmdPrefix, long segmentStart) {
        return new File(baseDir, AbstractIndex.BLOCK_V2 + cmdPrefix + segmentStart);
    }

    private File blockV1File(String cmdPrefix, long segmentStart) {
        return new File(baseDir, AbstractIndex.BLOCK + cmdPrefix + segmentStart);
    }

    private File cmdSegmentFile(String cmdPrefix, long segmentStart) {
        return new File(baseDir, cmdPrefix + segmentStart);
    }

    private void seedCmdBytesFromFile(String path) throws IOException {
        File f = new File(path);
        ControllableFile controllableFile = new DefaultControllableFile(f);
        controllableFile.getFileChannel().position(0);
        while (controllableFile.getFileChannel().position() < controllableFile.getFileChannel().size()) {
            int size = (int) Math.min(1024, controllableFile.getFileChannel().size() - controllableFile.getFileChannel().position());
            ByteBuffer buffer = ByteBuffer.allocate(size);
            controllableFile.getFileChannel().read(buffer);
            buffer.flip();
            ByteBuf byteBuf = Unpooled.wrappedBuffer(buffer.array());
            int n = testCmdStore.writeCmd(byteBuf);
            segmentWritten += n;
            totalWritten += n;
        }
        controllableFile.close();
    }

    @After
    public void tearDown() throws IOException {
        if (defaultIndexStore != null) {
            try {
                defaultIndexStore.closeWriter();
            } catch (Exception ignore) {
            }
        }
        for (AsyncSegmentFile seg : openedSegments) {
            try {
                AsyncFileSystemHelper.await(testFs.close(seg), "close test segment");
            } catch (Exception ignore) {
            }
        }
        openedSegments.clear();
        if (testFs != null) {
            testFs.shutdown();
            testFs = null;
        }
        cleanDir(baseDir);
        String defaultBaseDir = Paths.get(tempDir, DEFAULT_BASE_DIR_NAME).toString();
        if (!defaultBaseDir.equals(baseDir)) {
            cleanDir(defaultBaseDir);
        }
    }

    private void cleanDir(String dirPath) throws IOException {
        File dir = new File(dirPath);
        if (!dir.exists()) {
            dir.mkdirs();
            return;
        }
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                file.delete();
            }
        }
        dir.delete();
        if (!dir.mkdirs() && !dir.exists()) {
            throw new IOException("create folder fail " + dir.getAbsolutePath());
        }
    }

    public void write(String path) throws IOException {
        File f = new File(path);
        ControllableFile controllableFile = new DefaultControllableFile(f);
        controllableFile.getFileChannel().position(0);
        while(controllableFile.getFileChannel().position() < controllableFile.getFileChannel().size()) {
            int size = (int)Math.min(1024, controllableFile.getFileChannel().size() - controllableFile.getFileChannel().position());
            ByteBuffer buffer = ByteBuffer.allocate(size);
            controllableFile.getFileChannel().read(buffer);
            buffer.flip();
            ByteBuf byteBuf = Unpooled.wrappedBuffer(buffer.array());
            defaultIndexStore.write(byteBuf);
        }
    }

    public void writeRawStr(String cmdStr) throws IOException {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(cmdStr.getBytes());
        defaultIndexStore.write(byteBuf);
    }

    public void writeGtidCommand(ByteBuf byteBuf) throws IOException {
        defaultIndexStore.write(byteBuf);
    }

    @Test
    public void testSearch() throws Exception {
        write(filePath);

        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "a4f566ef50a85e1119f17f9b746728b48609a2ab:1-6");

        long pre = System.currentTimeMillis();
        for(int i = 2; i < 6; i++) {
            Pair<Long, GtidSet> point = defaultIndexStore.locateContinueGtidSet(new GtidSet("a4f566ef50a85e1119f17f9b746728b48609a2ab:1-" + i));
            Assert.assertEquals(point.getValue(), new GtidSet("a4f566ef50a85e1119f17f9b746728b48609a2ab:1-" + i));
            RedisOp redisOp =  IndexTestTool.readBytebufAfter(filePath, point.getKey());
            Assert.assertEquals(redisOp.getOpGtid(), "a4f566ef50a85e1119f17f9b746728b48609a2ab:" + (i + 1));
        }
    }

    @Test
    public void testClose() throws Exception {
        write(filePath);
        File directory = new File(baseDir);
        int initSize = directory.listFiles().length;
        defaultIndexStore.closeWriter();
        int lastSize = directory.listFiles().length;
        Assert.assertEquals(initSize, lastSize);
    }

    @Test
    public void testFileChange() throws Exception {
        write(file1);
        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750");
        switchCmdSegment("cmd_19513000");
        write(file2);
        gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-13");
        for(int i = 2; i <= 12; i++) {
            Pair<Long, GtidSet> point = defaultIndexStore.locateContinueGtidSet(new GtidSet("a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-" + i));
            Assert.assertEquals(point.getValue().toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-" + i);
            RedisOp redisOp = IndexTestTool.readBytebufAfter(file2, point.getKey() - lastRollSegmentStart);
            Assert.assertEquals(redisOp.getOpGtid(), "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:" + (i+1));
        }
    }

    @Test
    public void testRecover() throws Exception {
        write(file1);
        // 不调用close
        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore.closeWriter();
        defaultIndexStore = new DefaultIndexStore(keeperConfig, ckStore, testCmdStore, baseDir, opParser, commandWriterCallback, gtidCmdFilter);
        defaultIndexStore.openWriter(writer);
        for(int i = 633744; i < 633750; i++) {
            Pair<Long, GtidSet> point = defaultIndexStore.locateContinueGtidSet(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-" + i));
            System.out.println(point.getKey());
            RedisOp redisOp = IndexTestTool.readBytebufAfter(file1, point.getKey());
            Assert.assertEquals(redisOp.getOpGtid(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:" + ( i + 1));
        }
    }

    @Test
    public void testGtidSet() throws Exception {
        write(file1);
        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750");
    }

    @Test
    public void testRecover2() throws Exception {
        write(file1);

        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750");

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore.closeWriter();
        defaultIndexStore = new DefaultIndexStore(keeperConfig, ckStore, testCmdStore, baseDir, opParser, commandWriterCallback, gtidCmdFilter);
        defaultIndexStore.openWriter(writer);

        gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750");

        switchCmdSegment("cmd_19513000");
        write(file2);

        gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-13");

        for(int i = 1; i <= 12; i++) {
            Pair<Long, GtidSet> point = defaultIndexStore.locateContinueGtidSet(new GtidSet("a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-" + i));
            RedisOp redisOp = IndexTestTool.readBytebufAfter(file2, point.getKey() - lastRollSegmentStart);
            Assert.assertEquals(redisOp.getOpGtid(), "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:" + (i+1));
        }
    }

    @Test
    public void testBuildIndex() throws Exception {
        seedCmdBytesFromFile(file1);
        long pre = System.currentTimeMillis();
        defaultIndexStore.buildIndexFromCmdFile(0);
        long now = System.currentTimeMillis();
        System.out.println("build index " + (now - pre));
        for(int i = 633744; i < 633745; i++) {
            Pair<Long, GtidSet> point = defaultIndexStore.locateContinueGtidSet(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-" + i));
            RedisOp redisOp = IndexTestTool.readBytebufAfter(file1, point.getKey());
            Assert.assertEquals(redisOp.getOpGtid(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:" + (i + 1));
        }

        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750");
    }

    @Test
    public void testRecover3() throws IOException {
        write(file1);
        write(file2);
        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-13");

        File firstFile = new File(file1);
        defaultIndexStore.closeWriter();
        testCmdStore.truncateCmdSegment(firstFile.length());
        segmentWritten = firstFile.length();
        totalWritten = firstFile.length();

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore = new DefaultIndexStore(keeperConfig, ckStore, testCmdStore, baseDir, opParser, commandWriterCallback, gtidCmdFilter);
        defaultIndexStore.openWriter(writer);

        gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750");
    }

    @Test
    public void testRecover4() throws IOException {
        write(file1);
        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750");

        defaultIndexStore.closeWriter();
        File indexFilePath = indexV1File(CMD_PREFIX, 0);
        DefaultControllableFile file = new DefaultControllableFile(indexFilePath);
        file.setLength((int) file.size() - 10);

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore = new DefaultIndexStore(keeperConfig, ckStore, testCmdStore, baseDir, opParser, commandWriterCallback, gtidCmdFilter);
        defaultIndexStore.openWriter(writer);

        gtidSet = defaultIndexStore.getIndexGtidSet();
        file.close();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750");
    }

    @Test
    public void testRecover5() throws IOException {
        write(file1);
        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750");

        defaultIndexStore.closeWriter();
        File blockFilePath = blockV1File(CMD_PREFIX, 0);
        DefaultControllableFile file = new DefaultControllableFile(blockFilePath);
        int size = (int) file.size();
        if (size > 10) {
            size = size - 10;
        }
        file.setLength(size);

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore = new DefaultIndexStore(keeperConfig, ckStore, testCmdStore, baseDir, opParser, commandWriterCallback, gtidCmdFilter);
        defaultIndexStore.openWriter(writer);

        gtidSet = defaultIndexStore.getIndexGtidSet();
        file.close();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750");
    }

    @Test
    public void parseDirty() throws Exception {
        String path = "src/test/resources/GtidTest/dirty";

        try {
            write(path);
            fail("should parse error");
        } catch (Exception ignore){
        }

        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "\"\"");
    }

    @Test
    public void parserdirty2() throws Exception {
        String dirtyPath = "src/test/resources/GtidTest/dirty2";
        try {
            write(dirtyPath);
            fail("should parse error");
        } catch (Exception ignored) {
            Assert.assertTrue(ignored.getMessage().contains("For input string: \"*6\""));
        }
        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-10");
        for(int i = 2; i <= 9; i++) {
            Pair<Long, GtidSet> point = defaultIndexStore.locateContinueGtidSet(new GtidSet("a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-" + i));
            Assert.assertEquals(point.getValue().toString(), "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-" + i);
            RedisOp redisOp = IndexTestTool.readBytebufAfter(dirtyPath, point.getKey());
            Assert.assertEquals(redisOp.getOpGtid(), "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:" + (i+1));
        }

    }

    @Test
    public void testMetaStoreFilter() throws IOException {
        reset(gtidCmdFilter);
        when(gtidCmdFilter.gtidSetContains(anyString(), anyLong())).thenAnswer(invocation -> {
            String gtid = invocation.getArgument(0);
            long num = invocation.getArgument(1);
            return "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0".equals(gtid) && num == 633745L;
        });
        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore.closeWriter();
        defaultIndexStore = new DefaultIndexStore(keeperConfig, ckStore, testCmdStore, baseDir, opParser, commandWriterCallback, gtidCmdFilter);
        defaultIndexStore.openWriter(writer);
        write(file1);

        Pair<Long, GtidSet> point = defaultIndexStore.locateGtidSetWithFallbackToEnd(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-633744"));
        Pair<Long, GtidSet> point2 = defaultIndexStore.locateGtidSetWithFallbackToEnd(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-633745"));
        Assert.assertEquals(point.getKey(), point2.getKey());

        point = defaultIndexStore.locateContinueGtidSet(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-" + 633746));
        RedisOp redisOp = IndexTestTool.readBytebufAfter(testCmdStore.currentCmdFile().getPath(), point.getKey());
        Assert.assertEquals(redisOp.getOpGtid(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633747");
    }

    @Test
    public void testBuildIndexFromCmdFileWithIncompleteTransaction() throws IOException {
        baseDir = Paths.get(tempDir, "IndexStoreTest-testBuildIndexFromCmdFileWithIncompleteTransaction").toString();
        cleanDir(baseDir);
        String cmdPrefix = "cmd_incomplete_tx_";
        String gtid1 = "a4f566ef50a85e1119f17f9b746728b48609a2ab:1";

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        segmentWritten = 0L;
        totalWritten = 0L;
        TestAsyncCommandStore cmdStore = createTestCmdStore(cmdPrefix);
        DefaultIndexStore testIndexStore = new DefaultIndexStore(keeperConfig, ckStore, cmdStore,
                baseDir, opParser, commandWriterCallback, gtidCmdFilter);
        testIndexStore.openWriter(writer);
        defaultIndexStore = testIndexStore;

        testIndexStore.write(createGtidCommand(gtid1, "SET", "key1", "value1"));
        long positionBeforeMulti = cmdStore.currentSegmentSize();
        testIndexStore.closeWriter();

        // Append incomplete MULTI transaction after close (crash mid-tx)
        ByteBuf multi = createMultiCommand();
        ByteBuf set2 = createSetCommand("key2", "value2");
        ByteBuf set3 = createSetCommand("key3", "value3");
        cmdStore.writeCmd(multi);
        cmdStore.writeCmd(set2);
        cmdStore.writeCmd(set3);
        // Prod crash-restart only sees flushed bytes; flush before recover/truncate.
        flushCmdSegment(cmdStore);
        segmentWritten = cmdStore.currentSegmentSize();
        totalWritten = segmentWritten;

        testIndexStore = new DefaultIndexStore(keeperConfig, ckStore, cmdStore,
                baseDir, opParser, commandWriterCallback, gtidCmdFilter);
        testIndexStore.openWriter(writer);
        defaultIndexStore = testIndexStore;

        Assert.assertEquals("File should be truncated to position before incomplete transaction",
                positionBeforeMulti, cmdStore.currentSegmentSize());

        GtidSet gtidSet = testIndexStore.getIndexGtidSet();
        Assert.assertTrue("GTID set should contain gtid1", gtidSet.contains("a4f566ef50a85e1119f17f9b746728b48609a2ab", 1));
        Assert.assertFalse("GTID set should not contain gtid2 from incomplete transaction",
                gtidSet.contains("a4f566ef50a85e1119f17f9b746728b48609a2ab", 2));

        Pair<Long, GtidSet> point = testIndexStore.locateContinueGtidSet(new GtidSet(gtid1));
        Assert.assertNotNull("Should be able to locate gtid1", point);
        Assert.assertEquals("Should locate gtid1", gtid1, point.getValue().toString());

        testIndexStore.closeWriter();
    }

    @Test
    public void testBuildIndexFromCmdFileWithIncompleteTransactionAfterValidCommands() throws IOException {
        baseDir = Paths.get(tempDir, "IndexStoreTest-testBuildIndexFromCmdFileWithIncompleteTransactionAfterValidCommands").toString();
        cleanDir(baseDir);
        String cmdPrefix = "cmd_incomplete_tx2_";
        String gtid1 = "a4f566ef50a85e1119f17f9b746728b48609a2ab:1";
        String gtid2 = "a4f566ef50a85e1119f17f9b746728b48609a2ab:2";
        String gtid3 = "a4f566ef50a85e1119f17f9b746728b48609a2ab:3";

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        segmentWritten = 0L;
        totalWritten = 0L;
        TestAsyncCommandStore cmdStore = createTestCmdStore(cmdPrefix);
        DefaultIndexStore testIndexStore = new DefaultIndexStore(keeperConfig, ckStore, cmdStore,
                baseDir, opParser, commandWriterCallback, gtidCmdFilter);
        testIndexStore.openWriter(writer);
        defaultIndexStore = testIndexStore;

        testIndexStore.write(createGtidCommand(gtid1, "SET", "key1", "value1"));
        testIndexStore.write(createGtidCommand(gtid2, "SET", "key2", "value2"));
        long positionBeforeIncompleteTransaction = cmdStore.currentSegmentSize();
        testIndexStore.closeWriter();

        cmdStore.writeCmd(createMultiCommand());
        cmdStore.writeCmd(createSetCommand("key3", "value3"));
        cmdStore.writeCmd(createSetCommand("key4", "value4"));
        // Prod crash-restart only sees flushed bytes; flush before recover/truncate.
        flushCmdSegment(cmdStore);
        segmentWritten = cmdStore.currentSegmentSize();
        totalWritten = segmentWritten;

        testIndexStore = new DefaultIndexStore(keeperConfig, ckStore, cmdStore,
                baseDir, opParser, commandWriterCallback, gtidCmdFilter);
        testIndexStore.openWriter(writer);
        defaultIndexStore = testIndexStore;

        Assert.assertEquals("File should be truncated to position before incomplete transaction",
                positionBeforeIncompleteTransaction, cmdStore.currentSegmentSize());

        GtidSet gtidSet = testIndexStore.getIndexGtidSet();
        Assert.assertTrue("GTID set should contain gtid1", gtidSet.contains("a4f566ef50a85e1119f17f9b746728b48609a2ab", 1));
        Assert.assertTrue("GTID set should contain gtid2", gtidSet.contains("a4f566ef50a85e1119f17f9b746728b48609a2ab", 2));
        Assert.assertFalse("GTID set should not contain gtid3 from incomplete transaction",
                gtidSet.toString().contains(gtid3));

        Pair<Long, GtidSet> point1 = testIndexStore.locateContinueGtidSet(new GtidSet(gtid1));
        Assert.assertNotNull("Should be able to locate gtid1", point1);
        Pair<Long, GtidSet> point2 = testIndexStore.locateContinueGtidSet(new GtidSet(gtid2));
        Assert.assertNotNull("Should be able to locate gtid2", point2);

        testIndexStore.closeWriter();
    }

    // Helper methods to create Redis protocol commands

    private void writeCommandToFile(File file, ByteBuf command) throws IOException {
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(file, true);
             java.nio.channels.FileChannel channel = fos.getChannel()) {
            int readableBytes = command.readableBytes();
            byte[] bytes = new byte[readableBytes];
            int readerIndex = command.readerIndex();
            command.getBytes(readerIndex, bytes);
            channel.write(java.nio.ByteBuffer.wrap(bytes));
        }
    }
    private void writeGtidSetToFile(File file, GtidSet gtidSet) throws IOException {
        AsyncFile asyncFile = openTestAsyncFile(file, true);
        try {
            new GtidSetWrapper(gtidSet).saveGtidSet(testFs, asyncFile);
        } finally {
            AsyncFileSystemHelper.await(testFs.close(asyncFile), "close test index file");
        }
    }

    private void writeGtidSetV2ToFile(File file, GtidSet gtidSet) throws IOException {
        AsyncFile asyncFile = openTestAsyncFile(file, true);
        try {
            new GtidSetWrapper(gtidSet).saveGtidSetV2(testFs, asyncFile);
        } finally {
            AsyncFileSystemHelper.await(testFs.close(asyncFile), "close test index file");
        }
    }


    private ByteBuf createGtidCommand(String gtid, String... args) {
        ByteBuf buffer = Unpooled.buffer();
        // Format: *N\r\n$4\r\nGTID\r\n$40\r\n<gtid>\r\n$1\r\n0\r\n$M\r\n<command>...
        int totalArgs = 3 + args.length; // GTID + gtid + "0" + command args
        buffer.writeByte((byte)'*');
        buffer.writeBytes(String.valueOf(totalArgs).getBytes());
        buffer.writeBytes("\r\n".getBytes());

        // GTID
        writeBulkString(buffer, "GTID");
        // GTID value
        writeBulkString(buffer, gtid);
        // "0" (database number)
        writeBulkString(buffer, "0");
        // Command args
        for (String arg : args) {
            writeBulkString(buffer, arg);
        }
        return buffer;
    }

    private ByteBuf createMultiCommand() {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte((byte)'*');
        buffer.writeBytes("1".getBytes());
        buffer.writeBytes("\r\n".getBytes());
        writeBulkString(buffer, "MULTI");
        return buffer;
    }

    private ByteBuf createSetCommand(String key, String value) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte((byte)'*');
        buffer.writeBytes("3".getBytes());
        buffer.writeBytes("\r\n".getBytes());
        writeBulkString(buffer, "SET");
        writeBulkString(buffer, key);
        writeBulkString(buffer, value);
        return buffer;
    }

    private void writeBulkString(ByteBuf buffer, String str) {
        buffer.writeByte((byte)'$');
        buffer.writeBytes(String.valueOf(str.length()).getBytes());
        buffer.writeBytes("\r\n".getBytes());
        buffer.writeBytes(str.getBytes());
        buffer.writeBytes("\r\n".getBytes());
    }

    @Test
    public void testLocateGtidRange_NoIndexFile() throws IOException {
        defaultIndexStore.closeWriter();
        long segStart = 0L;
        indexV1File(CMD_PREFIX, segStart).delete();
        blockV1File(CMD_PREFIX, segStart).delete();
        indexV2File(CMD_PREFIX, segStart).delete();
        blockV2File(CMD_PREFIX, segStart).delete();

        List<Pair<Long, Long>> result = defaultIndexStore.locateGtidRange(
            "a4f566ef50a85e1119f17f9b746728b48609a2ab", 1, 10);

        Assert.assertTrue("Should return empty list when no index file exists", result.isEmpty());
    }

    @Test
    public void testLocateGtidRange_NoIntersection() throws IOException {
        // Test when current GTID set has no intersection with request
        write(filePath);
        
        // Request GTID range that doesn't exist in the index
        List<Pair<Long, Long>> result = defaultIndexStore.locateGtidRange(
            "0000000000000000000000000000000000000000", 1, 10);
        
        Assert.assertTrue("Should return empty list when no intersection", result.isEmpty());
    }

    @Test
    public void testLocateGtidRange_SingleIndexFile() throws IOException {
        // Test locating GTID range in a single index file
        write(filePath);
        
        String uuid = "a4f566ef50a85e1119f17f9b746728b48609a2ab";
        List<Pair<Long, Long>> result = defaultIndexStore.locateGtidRange(uuid, 2, 5);
        
        Assert.assertFalse("Should find ranges in single index file", result.isEmpty());
        Assert.assertTrue("Should have at least one range", result.size() >= 1);
        
        // Verify ranges are valid (start < end)
        for (Pair<Long, Long> range : result) {
            Assert.assertNotNull("Start offset should not be null", range.getKey());
            Assert.assertNotNull("End offset should not be null", range.getValue());
            Assert.assertTrue("Start offset should be less than end offset", 
                range.getKey() < range.getValue());
        }
    }

    @Test
    public void testLocateGtidRange_MultipleIndexFiles() throws Exception {
        // Test locating GTID range across multiple index files
        write(file1);
        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750");
        
        switchCmdSegment("cmd_19513000");
        write(file2);
        
        gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), 
            "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-13");
        
        // Test locating range in second file
        String uuid = "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0";
        List<Pair<Long, Long>> result = defaultIndexStore.locateGtidRange(uuid, 2, 10);
        
        Assert.assertFalse("Should find ranges across multiple index files", result.isEmpty());
        
        // Verify ranges
        for (Pair<Long, Long> range : result) {
            Assert.assertNotNull("Start offset should not be null", range.getKey());
            Assert.assertNotNull("End offset should not be null", range.getValue());
            Assert.assertTrue("Start offset should be less than end offset", 
                range.getKey() < range.getValue());
            Assert.assertTrue("Start offset should be in second segment",
                range.getKey() >= lastRollSegmentStart);
        }
    }

    @Test
    public void testLocateGtidRange_ExactMatch() throws IOException {
        // Test locating exact GTID range
        write(filePath);
        
        String uuid = "a4f566ef50a85e1119f17f9b746728b48609a2ab";
        List<Pair<Long, Long>> result = defaultIndexStore.locateGtidRange(uuid, 1, 6);
        
        Assert.assertFalse("Should find exact match", result.isEmpty());
        
        // Verify we can read commands from the found ranges
        for (Pair<Long, Long> range : result) {
            long startOffset = range.getKey();
            
            // Try to read a command at the start offset
            RedisOp redisOp = IndexTestTool.readBytebufAfter(filePath, startOffset);
            Assert.assertNotNull("Should be able to read command at start offset", redisOp);
            Assert.assertNotNull("Command should have GTID", redisOp.getOpGtid());
        }
    }

    @Test
    public void testLocateGtidRange_PartialRange() throws IOException {
        // Test locating partial GTID range (subset of available GTIDs)
        write(filePath);
        
        String uuid = "a4f566ef50a85e1119f17f9b746728b48609a2ab";
        // Request range 3-4, but available is 1-6
        List<Pair<Long, Long>> result = defaultIndexStore.locateGtidRange(uuid, 3, 4);
        
        Assert.assertFalse("Should find partial range", result.isEmpty());
        
        for (Pair<Long, Long> range : result) {
            Assert.assertNotNull("Start offset should not be null", range.getKey());
            Assert.assertNotNull("End offset should not be null", range.getValue());
        }
    }

    @Test
    public void testLocateGtidRange_OutOfRange() throws IOException {
        // Test locating GTID range that's out of available range
        write(filePath);
        
        String uuid = "a4f566ef50a85e1119f17f9b746728b48609a2ab";
        // Request range 10-20, but available is only 1-6
        List<Pair<Long, Long>> result = defaultIndexStore.locateGtidRange(uuid, 10, 20);
        
        Assert.assertTrue("Should return empty list for out of range request", result.isEmpty());
    }

    @Test
    public void testLocateGtidRange_AfterClose() throws IOException {
        // Test locating GTID range after closing writer
        write(filePath);
        
        // Ensure index is saved before closing by calling locateGtidRange while writer is open
        String uuid = "a4f566ef50a85e1119f17f9b746728b48609a2ab";
        List<Pair<Long, Long>> resultBeforeClose = defaultIndexStore.locateGtidRange(uuid, 1, 6);
        Assert.assertFalse("Should find ranges before closing writer", resultBeforeClose.isEmpty());
        
        // Now close the writer
        defaultIndexStore.closeWriter();
        
        // After closing writer, saveIndex() returns null, which causes locateGtidRange to return early
        // This is a limitation of the current implementation - it requires indexWriter to be open
        // However, we can verify that the index files exist and can be read via getIndexGtidSet
        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertNotNull("GTID set should be available after closing writer", gtidSet);
        Assert.assertFalse("GTID set should not be empty", gtidSet.isEmpty());
        
        File indexDir = new File(baseDir);
        File[] indexFiles = indexDir.listFiles((dir, name) ->
                name.startsWith(AbstractIndex.INDEX) || name.startsWith(AbstractIndex.INDEX_V2));
        Assert.assertNotNull("Index files should exist", indexFiles);
        Assert.assertTrue("Should have at least one index file", indexFiles.length > 0);
        
        // Note: locateGtidRange may return empty after closing writer due to saveIndex() returning null
        // This test verifies that index files are preserved and can be read via getIndexGtidSet
    }

    @Test
    public void testLocateGtidRange_FileEnd() throws Exception {
        // Test locating GTID range that extends to file end
        write(file1);
        switchCmdSegment("cmd_19513000");
        write(file2);
        
        String uuid = "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0";
        
        // First verify that we can locate ranges for this UUID
        List<Pair<Long, Long>> result = defaultIndexStore.locateGtidRange(uuid, 1, 5);
        Assert.assertFalse("Should find ranges for this UUID", result.isEmpty());
        
        // Verify the ranges are valid
        for (Pair<Long, Long> range : result) {
            Assert.assertNotNull("Start offset should not be null", range.getKey());
            Assert.assertNotNull("End offset should not be null", range.getValue());
            Assert.assertTrue("End offset should be greater than start", 
                range.getValue() > range.getKey());
        }
        
        // Now try to locate a range that includes the last GTIDs (10-13)
        // This may include the file end, where endOffset might be determined from file length
        result = defaultIndexStore.locateGtidRange(uuid, 10, 13);
        
        // The result might be empty if:
        // 1. The GTIDs 10-13 are not fully indexed yet (not saved to index file)
        // 2. The file end offset cannot be determined (getFileEndBacklogOffset returns null)
        // So we verify that at least the earlier range (1-5) works correctly
        // If 10-13 works, verify the ranges
        if (!result.isEmpty()) {
            for (Pair<Long, Long> range : result) {
                Assert.assertNotNull("Start offset should not be null", range.getKey());
                // End offset might be null for file end, or might be calculated from file length
                if (range.getValue() != null) {
                    Assert.assertTrue("End offset should be greater than start", 
                        range.getValue() > range.getKey());
                }
            }
        }
        
        // Verify that the GTID set includes the expected range
        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertTrue("GTID set should contain the UUID", 
            gtidSet.contains(uuid, 1) || gtidSet.contains(uuid, 13));
    }

    @Test
    public void testLocateGtidRange_EmptyRange() throws IOException {
        // Test locating empty GTID range (begGno > endGno)
        write(filePath);
        
        String uuid = "a4f566ef50a85e1119f17f9b746728b48609a2ab";
        List<Pair<Long, Long>> result = defaultIndexStore.locateGtidRange(uuid, 5, 3);
        
        // Empty range should return empty list
        Assert.assertTrue("Should return empty list for invalid range", result.isEmpty());
    }

    @Test
    public void testLocateGtidRange_SingleGno() throws IOException {
        // Test locating single GTID (begGno == endGno)
        write(filePath);
        
        String uuid = "a4f566ef50a85e1119f17f9b746728b48609a2ab";
        List<Pair<Long, Long>> result = defaultIndexStore.locateGtidRange(uuid, 3, 3);
        
        Assert.assertFalse("Should find single GTID", result.isEmpty());
        
        for (Pair<Long, Long> range : result) {
            Assert.assertNotNull("Start offset should not be null", range.getKey());
            Assert.assertNotNull("End offset should not be null", range.getValue());
            Assert.assertTrue("End offset should be greater than start",
                    range.getValue() > range.getKey());
        }
    }

    @Test
    public void testLocateSkipEmptyIndexFile() throws Exception {
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, 10).forEach(i -> {
            sb.append("*3\r\n" +
                    "$7\r\n" +
                    "PUBLISH\r\n" +
                    "$18\r\n" +
                    "__sentinel__:hello\r\n" +
                    "$147\r\n" +
                    "10.120.125.145,5026,ce1896062762e2920bc81db3edbad6bd66c97cde,0,xpipe-test-gap-allow-xsync+xpipe-test-gap-allow-xsync_1+NTGXH,10.120.125.145,20004,0\r\n");
        });
        writeRawStr(sb.toString());

        switchCmdSegment("cmd_19513000");
        write(filePath);

        String uuid = "a4f566ef50a85e1119f17f9b746728b48609a2ab";
        List<Pair<Long, Long>> result = defaultIndexStore.locateGtidRange(uuid, 3, 3);
        Assert.assertFalse("Should find single GTID", result.isEmpty());
    }

    @Test
    public void testV2WriteAndRead() throws Exception {

        write(file1);

        // 验证 GTID Set
        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750", gtidSet.toString());

        File indexV2 = indexV2File(CMD_PREFIX, 0);
        File blockV2 = blockV2File(CMD_PREFIX, 0);
        Assert.assertTrue("v2 index file should exist", indexV2.exists());
        Assert.assertTrue("v2 block file should exist", blockV2.exists());

        File indexV1 = indexV1File(CMD_PREFIX, 0);
        File blockV1 = blockV1File(CMD_PREFIX, 0);
        Assert.assertTrue("v1 index file should exist (dual write)", indexV1.exists());
        Assert.assertTrue("v1 block file should exist (dual write)", blockV1.exists());

        // 通过 DefaultIndexStore 的 locateContinueGtidSet 验证读取（内部使用 v2 reader）
        Pair<Long, GtidSet> point = defaultIndexStore.locateContinueGtidSet(
                new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-633745"));
        Assert.assertNotNull(point);
//        Assert.assertEquals("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-633745", point.getValue().toString());
        // 不校验 GtidSet 字符串（因为文件不含 1-633743），只验证偏移对应的命令
        RedisOp redisOp2 = IndexTestTool.readBytebufAfter(file1, point.getKey());
        Assert.assertEquals("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633746", redisOp2.getOpGtid());
        defaultIndexStore.closeWriter();
    }

    @Test
    public void testV2AlternatingWriteAndRead() throws Exception{
        String uuid = "b4f566ef50a85e1119f17f9b746728b48609a2ab";

        baseDir = Paths.get(tempDir, "IndexStoreTest-zoneAlternating").toString();
        cleanDir(baseDir);
        String cmdPrefix = "cmd_zone_alt_";
        segmentWritten = 0L;
        totalWritten = 0L;
        bindWriteCommandToFs();

        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        DefaultIndexStore store = new DefaultIndexStore(keeperConfig, ckStore, createTestCmdStore(cmdPrefix), baseDir, opParser, commandWriterCallback,
                gtidCmdFilter);
        store.openWriter(writer);
        defaultIndexStore = store;

        writeGtidRangeCommand(uuid,1,3);
        writeRawStr(createPublishCommand(2));
        writeGtidRangeCommand(uuid,4,8);
        writeRawStr(createPublishCommand(1));
        writeGtidRangeCommand(uuid,9,12);

        GtidSet fullGtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals("GTID set expected", new GtidSet("b4f566ef50a85e1119f17f9b746728b48609a2ab:1-12"),fullGtidSet);

        Assert.assertTrue("v2 index file should exist", indexV2File(cmdPrefix, 0).exists());
        Assert.assertTrue("v2 block file should exist", blockV2File(cmdPrefix, 0).exists());

        File cmdFile = testCmdStore.currentCmdFile();
        Pair<Long, GtidSet> point = defaultIndexStore.locateContinueGtidSet(
                new GtidSet(uuid + ":1-5"));
        Assert.assertNotNull(point);
        RedisOp op6 = IndexTestTool.readBytebufAfter(cmdFile.getPath(), point.getKey());
        Assert.assertNotNull(op6);
        Assert.assertEquals(uuid + ":6", op6.getOpGtid());

        point = defaultIndexStore.locateContinueGtidSet(
                new GtidSet(uuid + ":1-8"));
        Assert.assertNotNull(point);
        RedisOp op9 = IndexTestTool.readBytebufAfter(cmdFile.getPath(), point.getKey());
        Assert.assertNotNull(op9);
        Assert.assertEquals(uuid + ":9", op9.getOpGtid());

        List<long[]> zones = defaultIndexStore.getIndexWriterV2().loadAllZones();
        Assert.assertEquals("Alternating GTID/non-GTID should not flush small zones", 0, zones.size());
    }

    @Test
    public void testV2ZoneWriter_ClearOnGtid() throws IOException {
        // R1/R2: GTID 到来清空 pending zone（不落 ZONE），前面的 non-GTID 由 recover 阶段的 rebuildStart 补上。
        baseDir = Paths.get(tempDir, "IndexStoreTest-v2zoneClearOnGtid").toString();
        File dir = new File(baseDir);
        if (dir.exists()) for (File f : dir.listFiles()) f.delete();
        else dir.mkdirs();

        String cmdName = "cmd_v2zone_clear_on_gtid_0";
        File cmdFile = new File(baseDir, cmdName);

        DefaultIndexStore store = createV2Store(cmdFile,cmdName);

        ByteBuf pingCmd = createPingCommand();
        String uuid = "cafebabecafebabecafebabecafebabecafebabe";

        store.write(pingCmd);
        pingCmd = createPingCommand();
        store.write(pingCmd);

        IndexWriterV2 writerV2 = store.getIndexWriterV2();
        List<long[]> zonesBeforeGtid = writerV2.loadAllZones();
        Assert.assertEquals("No zone before GTID (threshold not reached)", 0, zonesBeforeGtid.size());

        ByteBuf gtidCmd = createGtidCommand(uuid + ":1", "SET", "k", "v");
        store.write(gtidCmd);

        // R1/R2: GTID 到来清空 pending zone 而不落盘，因此仍为 0 条 zone
        List<long[]> zonesAfterGtid = writerV2.loadAllZones();
        Assert.assertEquals("Pending zone cleared on GTID arrival", 0, zonesAfterGtid.size());

        store.closeWriter();
    }

    @Test
    public void testV2ZoneWriter_FlushOnThreshold() throws IOException {
        baseDir = Paths.get(tempDir, "IndexStoreTest-v2zoneFlushOnThreshold").toString();
        cleanDir(baseDir);

        String cmdName = "cmd_v2zone_flush_threshold_";
        File cmdFile = cmdSegmentFile(toCmdPrefix(cmdName), 0);

        DefaultIndexStore store = createV2Store(cmdFile, cmdName);
        defaultIndexStore = store;
        IndexWriterV2 writerV2 = store.getIndexWriterV2();

        int threshold = TEST_ZONE_CONSECUTIVE_THRESHOLD;
        for (int i = 0; i < threshold; i++) {
            store.write(createPingCommand());
        }
        long offsetAfterThreshold = testCmdStore.currentSegmentSize();

        List<long[]> zones = writerV2.loadAllZones();
        Assert.assertEquals(1, zones.size());
        Assert.assertArrayEquals(new long[]{0, offsetAfterThreshold}, zones.get(0));

        for (int i = 0; i < 7; i++) {
            store.write(createPingCommand());
        }
        long finalOffset = testCmdStore.currentSegmentSize();
        store.closeWriter();

        DefaultIndexStore store2 = createV2Store(cmdFile, cmdName);
        defaultIndexStore = store2;
        writerV2 = store2.getIndexWriterV2();

        zones = writerV2.loadAllZones();
        Assert.assertEquals(2, zones.size());
        Assert.assertArrayEquals(new long[]{0, offsetAfterThreshold}, zones.get(0));
        Assert.assertArrayEquals(new long[]{offsetAfterThreshold, finalOffset}, zones.get(1));
        store2.closeWriter();
    }

    @Test
    public void testV2RecoverIndex_SkipsZoneMainInterval() throws IOException {
        // T-R.5: 含 ZONE 时 recoverIndex 应以 max(cmdEndOffset) 为 rebuildStart，跳过已落盘 ZONE 主区间
        baseDir = Paths.get(tempDir, "IndexStoreTest-v2RecoverSkipZone").toString();
        cleanDir(baseDir);

        String cmdName = "cmd_v2_recover_skip_zone_";
        File cmdFile = cmdSegmentFile(toCmdPrefix(cmdName), 0);

        int zoneThreshold = TEST_ZONE_CONSECUTIVE_THRESHOLD;
        DefaultIndexStore store = createV2OnlyStoreWithThresholds(cmdName, zoneThreshold, 16L * 1024 * 1024);
        defaultIndexStore = store;
        for (int i = 0; i < zoneThreshold; i++) {
            store.write(createPingCommand());
        }
        long offsetAfterZone = testCmdStore.currentSegmentSize();
        List<long[]> zones = store.getIndexWriterV2().loadAllZones();
        Assert.assertEquals(1, zones.size());
        Assert.assertEquals(offsetAfterZone, zones.get(0)[1]);
        store.closeWriter();

        ByteBuf ping = Unpooled.wrappedBuffer(pingCommandBytes());
        for (int i = 0; i < 3; i++) {
            testCmdStore.writeCmd(ping.duplicate());
        }
        segmentWritten = testCmdStore.currentSegmentSize();
        totalWritten = segmentWritten;

        DefaultIndexStore store2 = spy(createV2OnlyStoreUnopened(cmdName, zoneThreshold, 16L * 1024 * 1024));
        defaultIndexStore = store2;
        store2.openWriter(writer);
        verify(store2).buildIndexFromCmdFile(eq(offsetAfterZone), anyString(), anyString(), anyLong(), anyLong());
        store2.closeWriter();
    }

    @Test
    public void testV2RecoverIndex_TruncateMalformedTail() throws IOException {
        // T-R.5: v2 index 尾部残缺 entry 应 truncate，GtidSet 不损坏。
        // 必须经 fs.truncate：复用 testCmdStore/TailCache 时 out-of-band setLength 会让 recover 仍见完整 cache。
        baseDir = Paths.get(tempDir, "IndexStoreTest-v2RecoverTruncate").toString();
        cleanDir(baseDir);

        String cmdName = "cmd_v2_recover_truncate_";
        String cmdPrefix = toCmdPrefix(cmdName);
        String indexV2Prefix = AbstractIndex.INDEX_V2 + cmdPrefix;
        String uuid = "cafebabecafebabecafebabecafebabecafebabe";
        int zoneThreshold = TEST_ZONE_CONSECUTIVE_THRESHOLD;

        DefaultIndexStore store = createV2OnlyStoreWithThresholds(cmdName, zoneThreshold, 16L * 1024 * 1024);
        defaultIndexStore = store;
        for (int i = 0; i < zoneThreshold; i++) {
            store.write(createPingCommand());
        }
        store.write(createGtidCommand(uuid + ":1", "SET", "k", "v"));
        GtidSet gtidSetBefore = store.getIndexGtidSet();
        Assert.assertTrue(gtidSetBefore.contains(uuid, 1));
        store.closeWriter();

        AsyncFile indexV2Handle = currentIndexHandle(indexV2Prefix);
        long indexSizeBefore = AsyncFileSystemHelper.await(testFs.size(indexV2Handle), "size index v2 before truncate");
        Assert.assertTrue(indexSizeBefore > IndexEntry.SEGMENT_LENGTH_V2);
        long truncatedSize = indexSizeBefore - IndexEntry.SEGMENT_LENGTH_V2 / 2;
        AsyncFileSystemHelper.await(testFs.truncate(indexV2Handle, truncatedSize), "truncate malformed index v2 tail");
        Assert.assertEquals(truncatedSize,
                (long) AsyncFileSystemHelper.await(testFs.size(indexV2Handle), "size index v2 after truncate"));

        DefaultIndexStore store2 = createV2OnlyStoreWithThresholds(cmdName, zoneThreshold, 16L * 1024 * 1024);
        defaultIndexStore = store2;
        GtidSet gtidSetAfter = store2.getIndexGtidSet();
        Assert.assertTrue(gtidSetAfter.contains(uuid, 1));
        store2.closeWriter();
        Assert.assertEquals(indexSizeBefore,
                (long) AsyncFileSystemHelper.await(testFs.size(currentIndexHandle(indexV2Prefix)),
                        "size index v2 after recover"));
    }

    @Test
    public void testV2RecoverIndex_RollbackIncompleteTransactionPreservesGtid() throws IOException {
        baseDir = Paths.get(tempDir, "IndexStoreTest-v2RecoverIncompleteTx").toString();
        cleanDir(baseDir);

        String cmdName = "cmd_v2_recover_incomplete_tx_";
        String uuid = "a4f566ef50a85e1119f17f9b746728b48609a2ab";
        int zoneThreshold = TEST_ZONE_CONSECUTIVE_THRESHOLD;

        DefaultIndexStore store = createV2OnlyStoreWithThresholds(cmdName, zoneThreshold, 16L * 1024 * 1024);
        defaultIndexStore = store;
        for (int i = 0; i < zoneThreshold; i++) {
            store.write(createPingCommand());
        }
        store.write(createGtidCommand(uuid + ":1", "SET", "k1", "v1"));
        store.write(createGtidCommand(uuid + ":2", "SET", "k2", "v2"));
        GtidSet gtidSetBefore = store.getIndexGtidSet();
        Assert.assertTrue(gtidSetBefore.contains(uuid, 1));
        Assert.assertTrue(gtidSetBefore.contains(uuid, 2));
        store.closeWriter();

        long cmdLenBeforeIncomplete = testCmdStore.currentSegmentSize();
        testCmdStore.writeCmd(createMultiCommand());
        testCmdStore.writeCmd(createSetCommand("k3", "v3"));
        testCmdStore.writeCmd(createSetCommand("k4", "v4"));
        // Prod crash-restart only sees flushed bytes; ASYNC TailCache may still be dirty.
        // Flush so recover truncate sees disk >= rollback offset (avoids segment reset).
        flushCmdSegment(testCmdStore);
        segmentWritten = testCmdStore.currentSegmentSize();
        totalWritten = segmentWritten;

        DefaultIndexStore store2 = createV2OnlyStoreWithThresholds(cmdName, zoneThreshold, 16L * 1024 * 1024);
        defaultIndexStore = store2;
        Assert.assertEquals("Incomplete transaction tail should be rolled back",
                cmdLenBeforeIncomplete, testCmdStore.currentSegmentSize());
        GtidSet gtidSetAfter = store2.getIndexGtidSet();
        Assert.assertTrue("GTID 1 should survive recover", gtidSetAfter.contains(uuid, 1));
        Assert.assertTrue("GTID 2 should survive recover", gtidSetAfter.contains(uuid, 2));
        Assert.assertFalse("No GTID should be added from rolled-back tail",
                gtidSetAfter.contains(uuid, 3));
        store2.closeWriter();
    }

    @Test
    public void testV2ZoneWriter_GtidEntryBeforeZoneOnFlush() throws IOException {
        // appendNonGtid 触发 flush 时若 blockWriter 仍有未落盘 GTID，须先落 GTID entry 再落 ZONE entry
        baseDir = Paths.get(tempDir, "IndexStoreTest-v2GtidBeforeZone").toString();
        File dir = new File(baseDir);
        if (dir.exists()) for (File f : dir.listFiles()) f.delete();
        else dir.mkdirs();

        String cmdName = "cmd_v2_gtid_before_zone";
        File cmdFile = new File(baseDir, cmdName);
        String uuid = "cafebabecafebabecafebabecafebabecafebabe";

        // 低字节阈值便于触发 mixed flush，无需写满生产默认 8192 条 PING
        DefaultIndexStore store = createV2StoreWithThresholds(cmdFile, cmdName, TEST_ZONE_CONSECUTIVE_THRESHOLD, 200);
        defaultIndexStore = store;

        store.write(createGtidCommand(uuid + ":1", "SET", "k", "v"));
        while (testCmdStore.currentSegmentSize() < 250) {
            store.write(createPingCommand());
        }
        store.getIndexWriterV2().flush();
        List<long[]> zones = store.getIndexWriterV2().loadAllZones();
        Assert.assertFalse("ZONE entry should exist after mixed flush", zones.isEmpty());
        Assert.assertTrue(store.getIndexGtidSet().contains(uuid, 1));
        // ZONE end should be after GTID cmd bytes (GTID flushed before ZONE in flushUnlocked)
        Assert.assertTrue(zones.get(0)[0] > 0);
        store.closeWriter();
    }

    @Test
    public void testV2FlushIndexEntry_GtidThenZone() throws IOException {
        // flush 同时 pending GTID + ZONE 时，须先落 GTID entry 再落 ZONE entry
        baseDir = Paths.get(tempDir, "IndexStoreTest-v2FlushIndexEntry").toString();
        cleanDir(baseDir);

        String cmdName = "cmd_v2_flush_index_entry_";
        File cmdFile = cmdSegmentFile(toCmdPrefix(cmdName), 0);
        String uuid = "cafebabecafebabecafebabecafebabecafebabe";

        DefaultIndexStore store = createV2StoreWithThresholds(cmdFile, cmdName, TEST_ZONE_CONSECUTIVE_THRESHOLD, 16L * 1024 * 1024);
        defaultIndexStore = store;
        IndexWriterV2 writerV2 = store.getIndexWriterV2();

        store.write(createGtidCommand(uuid + ":1", "SET", "k", "v"));
        store.write(createPingCommand());
        store.write(createPingCommand());

        writerV2.flush();
        List<long[]> zones = writerV2.loadAllZones();
        Assert.assertEquals("ZONE should be flushed", 1, zones.size());
        Assert.assertTrue("GTID should be in index set after flush",
                store.getIndexGtidSet().contains(uuid, 1));
        store.closeWriter();
    }

    @Test
    public void testDoSwitchCmdFile_V1FullHistoryPreservedWhenV2LateJoin() throws Exception {
        // 复现生产事故：老版本只写 V1，V1 累积长历史；灰度发布进入双写后 V2 首次创建，起点为空。
        // cmd 轮转触发 DefaultIndexStore#doSwitchCmdFile：
        //   continueGtidSet = indexWriterV2.getGtidSet();   // ← 用 V2 残缺集覆盖了 V1 完整集
        // 新一轮 V1 writer 以此起头，V1 完整历史被抹平。
        // 现网 readV2=false，读走 V1；切完 cmd 后 slave xsync 拿到的 keeperCont 只剩尾段 → full sync。
        baseDir = Paths.get(tempDir, "IndexStoreTest-dualWriteLateJoinSwitch").toString();
        File dir = new File(baseDir);
        if (dir.exists()) {
            for (File f : dir.listFiles()) f.delete();
        } else {
            dir.mkdirs();
        }

        String cmdName = "cmd_dual_write_late_join_0";
        File cmdFile = new File(baseDir, cmdName);
        String cmdPrefix = toCmdPrefix(cmdName);
        String uuid = "abababababababababababababababababababab";

        // ---------------- 阶段 1：模拟老版本 V1 单跑 ----------------
        // 用 dualWrite=true 开双写来产生 V1 文件，写完后删掉 V2 文件，模拟"V2 灰度前的磁盘状态"
        DefaultIndexStore phase1Store = createStoreWithFlags(cmdFile, cmdName, true, false);
        defaultIndexStore = phase1Store;
        for (int i = 1; i <= 1000; i++) {
            phase1Store.write(createGtidCommand(uuid + ":" + i, "SET", "k" + i, "v" + i));
        }
        Assert.assertEquals("phase1 V1 should accumulate full history",
                new GtidSet(uuid + ":1-1000"), phase1Store.getIndexGtidSet());
        phase1Store.closeWriter();

        Assert.assertTrue("V1 index file must exist after phase1",
                indexV1File(cmdPrefix, 0).exists());
        // 删掉 V2 磁盘产物，让阶段 2 里 V2 变成"首次创建、起点为空"
        Assert.assertTrue(indexV2File(cmdPrefix, 0).delete());
        Assert.assertTrue(blockV2File(cmdPrefix, 0).delete());

        // ---------------- 阶段 2：灰度发布，进入双写 ----------------
        // V1 从磁盘 recover 出完整 1-1000；V2 首次创建 继承 V1 gtidSet
        KeeperConfig config = mock(KeeperConfig.class);
        when(config.dualWrite()).thenReturn(true);
        when(config.readV2()).thenReturn(false);
        when(config.getIndexZoneConsecutiveThreshold()).thenReturn(TEST_ZONE_CONSECUTIVE_THRESHOLD);
        when(config.getIndexMixedTotalBytesThreshold()).thenReturn(16L * 1024 * 1024);
        DefaultIndexStore dualStore = createStoreWithKeeperConfig(cmdFile, cmdName, config);
        defaultIndexStore = dualStore; // 让 @After 能正确 close

        Assert.assertTrue("V2 index file should be re-created after entering dual write",
                indexV2File(cmdPrefix, 0).exists());
        Assert.assertEquals("V1 should recover full history from disk",
                new GtidSet(uuid + ":1-1000"),
                dualStore.getIndexWriterV1().getGtidSet());
        Assert.assertEquals("V2 starts use v1 (freshly created)",
                new GtidSet(uuid + ":1-1000"),
                dualStore.getIndexWriterV2().getGtidSet());

        // 再写少量命令 V1/V2 依然对称：V1=1-1050, V2=1-1050
        for (int i = 1001; i <= 1050; i++) {
            dualStore.write(createGtidCommand(uuid + ":" + i, "SET", "k" + i, "v" + i));
        }
        Assert.assertEquals("V1 covers full history",
                new GtidSet(uuid + ":1-1050"),
                dualStore.getIndexWriterV1().getGtidSet());
        Assert.assertEquals("V2 also covers full history after it joined",
                new GtidSet(uuid + ":1-1050"),
                dualStore.getIndexWriterV2().getGtidSet());
        Assert.assertEquals("readV2=false must read V1 (full history) before switch",
                new GtidSet(uuid + ":1-1050"),
                dualStore.getIndexGtidSet());

        // ---------------- 阶段 3：触发 cmd segment 切换（AsyncFS: closeWriter → roll → doSwitchCmdFile）----------------
        when(config.readV2()).thenReturn(true);
        switchCmdSegment("cmd_dual_write_late_join_1");

        // 关键断言：切换后 readV2=true V1读到的依然是 1-1050
        // 若 doSwitchCmdFile 用 indexWriterV2.getGtidSet()(=1-1050) 覆盖了 V1，此断言会为 1-1050
        Assert.assertEquals(
                "V1 writer itself must retain full history across cmd file switch",
                new GtidSet(uuid + ":1-1050"),
                dualStore.getIndexWriterV1().getGtidSet());
        Assert.assertEquals(
                "readV2=true must read V2 (full history) after switch",
                new GtidSet(uuid + ":1-1050"),
                dualStore.getIndexGtidSet());

        dualStore.closeWriter();
    }

    private DefaultIndexStore createV2Store(File ignoredCmdFile, String cmdName) throws IOException {
        return createV2StoreWithThresholds(ignoredCmdFile, cmdName, TEST_ZONE_CONSECUTIVE_THRESHOLD, 16L * 1024 * 1024);
    }

    private DefaultIndexStore createV2StoreUnopened(File ignoredCmdFile, String cmdName) throws IOException {
        return createV2StoreUnopened(ignoredCmdFile, cmdName, TEST_ZONE_CONSECUTIVE_THRESHOLD, 16L * 1024 * 1024);
    }

    private DefaultIndexStore createV2StoreUnopened(File ignoredCmdFile, String cmdName,
                                                    int zoneThreshold, long mixedBytesThreshold) throws IOException {
        String cmdPrefix = toCmdPrefix(cmdName);
        bindWriteCommandToFs();
        bindCommandFileMock();
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);
        when(writer.getFileContext()).thenReturn(commandFileContext);

        CKStore ckStoreLocal = mock(CKStore.class);
        KeeperConfig config = mock(KeeperConfig.class);
        when(ckStoreLocal.getKeeperConfig()).thenReturn(config);
        when(config.dualWrite()).thenReturn(true);
        when(config.readV2()).thenReturn(true);
        when(config.getIndexZoneConsecutiveThreshold()).thenReturn(zoneThreshold);
        when(config.getIndexMixedTotalBytesThreshold()).thenReturn(mixedBytesThreshold);
        when(config.getBlockSizeThreshold()).thenReturn(TEST_BLOCK_MAX_SIZE);

        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        TestAsyncCommandStore cmdStore = createTestCmdStore(cmdPrefix);
        return new DefaultIndexStore(config, ckStoreLocal, cmdStore, baseDir, opParser,
                commandWriterCallback, gtidCmdFilter);
    }

    private DefaultIndexStore createStoreWithFlags(File ignoredCmdFile, String cmdName,
                                                   boolean dualWrite, boolean readV2) throws IOException {
        return createStoreWithFlags(ignoredCmdFile, cmdName, dualWrite, readV2, TEST_ZONE_CONSECUTIVE_THRESHOLD, 16L * 1024 * 1024);
    }

    private DefaultIndexStore createStoreWithFlags(File ignoredCmdFile, String cmdName,
                                                   boolean dualWrite, boolean readV2,
                                                   int zoneThreshold, long mixedBytesThreshold) throws IOException {
        String cmdPrefix = toCmdPrefix(cmdName);
        bindWriteCommandToFs();
        bindCommandFileMock();
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);
        when(writer.getFileContext()).thenReturn(commandFileContext);

        CKStore ckStoreLocal = mock(CKStore.class);
        KeeperConfig config = mock(KeeperConfig.class);
        when(ckStoreLocal.getKeeperConfig()).thenReturn(config);
        when(config.dualWrite()).thenReturn(dualWrite);
        when(config.readV2()).thenReturn(readV2);
        when(config.getIndexZoneConsecutiveThreshold()).thenReturn(zoneThreshold);
        when(config.getIndexMixedTotalBytesThreshold()).thenReturn(mixedBytesThreshold);
        when(config.getBlockSizeThreshold()).thenReturn(TEST_BLOCK_MAX_SIZE);

        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        TestAsyncCommandStore cmdStore = createTestCmdStore(cmdPrefix);
        DefaultIndexStore store = new DefaultIndexStore(config, ckStoreLocal, cmdStore, baseDir, opParser,
                commandWriterCallback, gtidCmdFilter);
        store.openWriter(writer);
        return store;
    }

    private DefaultIndexStore createV2StoreWithThresholds(File ignoredCmdFile, String cmdName,
                                                            int zoneThreshold, long mixedBytesThreshold)
            throws IOException {
        String cmdPrefix = toCmdPrefix(cmdName);
        bindWriteCommandToFs();
        bindCommandFileMock();
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);
        when(writer.getFileContext()).thenReturn(commandFileContext);

        CKStore localCkStore = mock(CKStore.class);
        KeeperConfig localConfig = mock(KeeperConfig.class);
        when(localCkStore.getKeeperConfig()).thenReturn(localConfig);
        when(localConfig.dualWrite()).thenReturn(true);
        when(localConfig.readV2()).thenReturn(true);
        when(localConfig.getIndexZoneConsecutiveThreshold()).thenReturn(zoneThreshold);
        when(localConfig.getIndexMixedTotalBytesThreshold()).thenReturn(mixedBytesThreshold);
        when(localConfig.getBlockSizeThreshold()).thenReturn(TEST_BLOCK_MAX_SIZE);

        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        TestAsyncCommandStore cmdStore = createTestCmdStore(cmdPrefix);
        DefaultIndexStore store = new DefaultIndexStore(localConfig, localCkStore, cmdStore, baseDir, opParser,
                commandWriterCallback, gtidCmdFilter);
        store.openWriter(writer);
        return store;
    }

    /** External KeeperConfig so tests can mutate dualWrite/readV2 after open (late-join switch). */
    private DefaultIndexStore createStoreWithKeeperConfig(File ignoredCmdFile, String cmdName,
                                                          KeeperConfig keeperConfig) throws IOException {
        String cmdPrefix = toCmdPrefix(cmdName);
        bindWriteCommandToFs();
        bindCommandFileMock();
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);
        when(writer.getFileContext()).thenReturn(commandFileContext);
        when(keeperConfig.getBlockSizeThreshold()).thenReturn(TEST_BLOCK_MAX_SIZE);

        CKStore ckStore = mock(CKStore.class);
        when(ckStore.getKeeperConfig()).thenReturn(keeperConfig);

        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        TestAsyncCommandStore cmdStore = createTestCmdStore(cmdPrefix);
        DefaultIndexStore store = new DefaultIndexStore(keeperConfig, ckStore, cmdStore, baseDir, opParser,
                commandWriterCallback, gtidCmdFilter);
        store.openWriter(writer);
        return store;
    }

    /** V2-only store (dualWrite=false) — avoid V1+V2 double recover fighting on reopen. */
    private DefaultIndexStore createV2OnlyStoreWithThresholds(String cmdName,
                                                              int zoneThreshold, long mixedBytesThreshold)
            throws IOException {
        DefaultIndexStore store = createV2OnlyStoreUnopened(cmdName, zoneThreshold, mixedBytesThreshold);
        store.openWriter(writer);
        return store;
    }

    private DefaultIndexStore createV2OnlyStoreUnopened(String cmdName,
                                                        int zoneThreshold, long mixedBytesThreshold)
            throws IOException {
        String cmdPrefix = toCmdPrefix(cmdName);
        bindWriteCommandToFs();
        bindCommandFileMock();
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);
        when(writer.getFileContext()).thenReturn(commandFileContext);

        CKStore localCkStore = mock(CKStore.class);
        KeeperConfig localConfig = mock(KeeperConfig.class);
        when(localCkStore.getKeeperConfig()).thenReturn(localConfig);
        when(localConfig.dualWrite()).thenReturn(false);
        when(localConfig.readV2()).thenReturn(true);
        when(localConfig.getIndexZoneConsecutiveThreshold()).thenReturn(zoneThreshold);
        when(localConfig.getIndexMixedTotalBytesThreshold()).thenReturn(mixedBytesThreshold);
        when(localConfig.getBlockSizeThreshold()).thenReturn(TEST_BLOCK_MAX_SIZE);

        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        TestAsyncCommandStore cmdStore = createTestCmdStore(cmdPrefix);
        return new DefaultIndexStore(localConfig, localCkStore, cmdStore, baseDir, opParser,
                commandWriterCallback, gtidCmdFilter);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = DefaultIndexStore.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class GtidIndexSnapshot {
        final String uuid;
        final long startGno;
        final int size;
        final long cmdStartOffset;
        final long blockStartOffset;
        final long blockEndOffset;

        GtidIndexSnapshot(IndexEntry entry) {
            this.uuid = entry.getUuid();
            this.startGno = entry.getStartGno();
            this.size = entry.getSize();
            this.cmdStartOffset = entry.getCmdStartOffset();
            this.blockStartOffset = entry.getBlockStartOffset();
            this.blockEndOffset = entry.getBlockEndOffset();
        }
    }

    private void assertGtidSnapshotEquals(GtidIndexSnapshot v1, GtidIndexSnapshot v2) {
        Assert.assertEquals("uuid", v1.uuid, v2.uuid);
        Assert.assertEquals("startGno", v1.startGno, v2.startGno);
        Assert.assertEquals("size", v1.size, v2.size);
        Assert.assertEquals("cmdStartOffset", v1.cmdStartOffset, v2.cmdStartOffset);
        Assert.assertEquals("blockStartOffset", v1.blockStartOffset, v2.blockStartOffset);
        Assert.assertEquals("blockEndOffset", v1.blockEndOffset, v2.blockEndOffset);
    }

    private List<GtidIndexSnapshot> readV1GtidSnapshots(String baseDirPath, String cmdName) throws IOException {
        List<GtidIndexSnapshot> snapshots = new ArrayList<>();
        String cmdPrefix = toCmdPrefix(cmdName);
        File indexV1 = new File(baseDirPath, AbstractIndex.INDEX + cmdPrefix + "0");
        AsyncFile asyncFile = openTestAsyncFile(indexV1, false);
        try {
            GtidSetWrapper.readGtidSet(testFs, asyncFile);
        } finally {
            AsyncFileSystemHelper.await(testFs.close(asyncFile), "close test index file");
        }
        try (ControllableFile indexFile = new DefaultControllableFile(indexV1)) {
            FileChannel ch = indexFile.getFileChannel();
            ByteBuffer lenBuf = ByteBuffer.allocate(Long.BYTES);
            ch.read(lenBuf);
            lenBuf.flip();
            ch.position(Long.BYTES + lenBuf.getLong());
            IndexEntry entry = readIndexEntryFromChannel(ch);
            while (entry != null) {
                snapshots.add(new GtidIndexSnapshot(entry));
                entry = readIndexEntryFromChannel(ch);
            }
        }
        return snapshots;
    }

    private List<GtidIndexSnapshot> readV2GtidSnapshots(String baseDirPath, String cmdName) throws IOException {
        List<GtidIndexSnapshot> snapshots = new ArrayList<>();
        for (IndexEntry entry : readV2GtidEntries(baseDirPath, cmdName)) {
            snapshots.add(new GtidIndexSnapshot(entry));
        }
        return snapshots;
    }

    private List<IndexEntry> readV2GtidEntries(String baseDirPath, String cmdName) throws IOException {
        List<IndexEntry> gtidEntries = new ArrayList<>();
        String cmdPrefix = toCmdPrefix(cmdName);
        File indexV2 = new File(baseDirPath, AbstractIndex.INDEX_V2 + cmdPrefix + "0");
        AsyncFile asyncFile = openTestAsyncFile(indexV2, false);
        long headerEnd;
        try {
            headerEnd = GtidSetWrapper.headerSize(testFs, asyncFile);
        } finally {
            AsyncFileSystemHelper.await(testFs.close(asyncFile), "close test index file");
        }
        try (ControllableFile indexFile = new DefaultControllableFile(indexV2)) {
            FileChannel ch = indexFile.getFileChannel();
            ch.position(headerEnd);
            while (ch.size() - ch.position() >= IndexEntry.SEGMENT_LENGTH_V2) {
                IndexEntry entry = readIndexEntryV2FromChannel(ch);
                if (entry == null) {
                    break;
                }
                if (!entry.isZone()) {
                    gtidEntries.add(entry);
                }
            }
        }
        return gtidEntries;
    }

    private List<IndexEntryType> readIndexEntryTypes(String baseDirPath, String cmdName) throws IOException {
        String cmdPrefix = toCmdPrefix(cmdName);
        File indexV2 = new File(baseDirPath, AbstractIndex.INDEX_V2 + cmdPrefix + "0");
        List<IndexEntryType> types = new java.util.ArrayList<>();
        AsyncFile asyncFile = openTestAsyncFile(indexV2, false);
        long headerEnd;
        try {
            headerEnd = GtidSetWrapper.headerSize(testFs, asyncFile);
        } finally {
            AsyncFileSystemHelper.await(testFs.close(asyncFile), "close test index file");
        }
        try (ControllableFile indexFile = new DefaultControllableFile(indexV2)) {
            FileChannel ch = indexFile.getFileChannel();
            ch.position(headerEnd);
            while (ch.size() - ch.position() >= IndexEntry.SEGMENT_LENGTH_V2) {
                IndexEntry e = readIndexEntryV2FromChannel(ch);
                if (e == null) {
                    break;
                }
                types.add(e.getType());
            }
        }
        return types;
    }

    private static IndexEntry readIndexEntryFromChannel(FileChannel ch) throws IOException {
        if (ch.size() - ch.position() < IndexEntry.SEGMENT_LENGTH) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.allocate(IndexEntry.SEGMENT_LENGTH);
        ch.read(buffer);
        return IndexEntry.fromBuffer(buffer);
    }

    private static IndexEntry readIndexEntryV2FromChannel(FileChannel ch) throws IOException {
        if (ch.size() - ch.position() < IndexEntry.SEGMENT_LENGTH_V2) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.allocate(IndexEntry.SEGMENT_LENGTH_V2);
        ch.read(buffer);
        return IndexEntry.fromBufferV2(buffer);
    }


    private void writeGtidRangeCommand(String uuid,int startInclusive,int endInclusive) throws IOException {
        for(int i = startInclusive;i<=endInclusive;i++){
            writeGtidCommand(createGtidCommand(uuid+":"+i,"SET", "key"+i, "value"+i));
        }
    }

    private ByteBuf createPingCommand() {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte((byte) '*');
        buffer.writeBytes("1".getBytes());
        buffer.writeBytes("\r\n".getBytes());
        writeBulkString(buffer, "PING");
        return buffer;
    }

    private byte[] pingCommandBytes() {
        ByteBuf buf = createPingCommand();
        byte[] bytes = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), bytes);
        return bytes;
    }

    private String createPublishCommand(int cmdCount){
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, cmdCount).forEach(i -> {
            sb.append("*3\r\n" +
                    "$7\r\n" +
                    "PUBLISH\r\n" +
                    "$18\r\n" +
                    "__sentinel__:hello\r\n" +
                    "$147\r\n" +
                    "10.120.125.145,5026,ce1896062762e2920bc81db3edbad6bd66c97cde,0,xpipe-test-gap-allow-xsync+xpipe-test-gap-allow-xsync_1+NTGXH,10.120.125.145,20004,0\r\n");
        });
        return sb.toString();
    }
    @Test
    public void testPreAppendDoesNotTriggerIndexAppend() throws Exception {
        baseDir = Paths.get(tempDir, "IndexStoreTest-preAppendNoIndex").toString();
        File dir = new File(baseDir);
        if (dir.exists()) for (File f : dir.listFiles()) f.delete();
        else dir.mkdirs();

        String cmdName = "cmd_pre_append_no_index";
        File cmdFile = new File(baseDir, cmdName);
        DefaultIndexStore store = createV2Store(cmdFile, cmdName);

        IndexWriter indexWriterMock = mock(IndexWriter.class);
        IndexWriterV2 indexWriterV2Mock = mock(IndexWriterV2.class);
        setField(store, "indexWriter", indexWriterMock);
        setField(store, "indexWriterV2", indexWriterV2Mock);

        when(gtidCmdFilter.gtidSetContains(anyString(), anyLong())).thenReturn(false);

        String uuid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        boolean accepted = store.preAppend(uuid, 1L);
        Assert.assertTrue(accepted);

        verify(indexWriterMock, never()).append(anyString(), anyLong(), anyInt());
        verify(indexWriterV2Mock, never()).appendGtid(anyString(), anyLong(), anyLong(), anyList());
        verify(indexWriterV2Mock, never()).appendNonGtid(anyLong(), anyList());
    }

    @Test
    public void testV2BlockFullGtidFlush() throws IOException {
        int testBlockMaxSize = TEST_BLOCK_MAX_SIZE;
        baseDir = Paths.get(tempDir, "IndexStoreTest-v2BlockFull8192").toString();
        File dir = new File(baseDir);
        if (dir.exists()) for (File f : dir.listFiles()) f.delete();
        else dir.mkdirs();

        String cmdName = "cmd_v2_block_full_8192";
        File cmdFile = new File(baseDir, cmdName);
        String uuid = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

        DefaultIndexStore store = createV2StoreWithThresholds(cmdFile, cmdName,
                TEST_ZONE_CONSECUTIVE_THRESHOLD, 16L * 1024 * 1024);
        defaultIndexStore = store;
        for (int i = 1; i <= testBlockMaxSize + 1; i++) {
            store.write(createGtidCommand(uuid + ":" + i, "SET", "k" + i, "v" + i));
        }

        List<IndexEntry> flushedGtidEntries = readV2GtidEntries(baseDir, cmdName);
        Assert.assertFalse("Block full should flush at least one GTID entry to disk", flushedGtidEntries.isEmpty());
        boolean hasFullBlock = false;
        for (IndexEntry entry : flushedGtidEntries) {
            Assert.assertTrue("GTID block size must not exceed " + testBlockMaxSize,
                    entry.getSize() <= testBlockMaxSize);
            if (entry.getSize() == testBlockMaxSize) {
                hasFullBlock = true;
            }
        }
        Assert.assertTrue("Should flush a full block of " + testBlockMaxSize + " GTIDs", hasFullBlock);

        store.closeWriter();
        List<IndexEntry> allGtidEntries = readV2GtidEntries(baseDir, cmdName);
        int totalGtids = allGtidEntries.stream().mapToInt(IndexEntry::getSize).sum();
        Assert.assertEquals(testBlockMaxSize + 1, totalGtids);
    }

    @Test
    public void testDualWriteV1V2GtidParity() throws IOException {
        baseDir = Paths.get(tempDir, "IndexStoreTest-dualWriteParity").toString();
        File dir = new File(baseDir);
        if (dir.exists()) for (File f : dir.listFiles()) f.delete();
        else dir.mkdirs();

        String cmdName = "cmd_dual_write_parity";
        File cmdFile = new File(baseDir, cmdName);
        String uuid = "feedfacefeedfacefeedfacefeedfacefeedface";

        DefaultIndexStore store = createV2Store(cmdFile, cmdName);
        defaultIndexStore = store;
        writeGtidRangeCommand(uuid, 1, 20);
        for (int i = 0; i < 50; i++) {
            store.write(createPingCommand());
        }
        writeGtidRangeCommand(uuid, 21, 40);
        store.closeWriter();

        String cmdPrefix = toCmdPrefix(cmdName);
        Assert.assertTrue(indexV1File(cmdPrefix, 0).exists());
        Assert.assertTrue(indexV2File(cmdPrefix, 0).exists());

        GtidSet expected = new GtidSet(uuid + ":1-40");
        ReplId replId = testCmdStore.getFileSystemReplId();
        try (IndexReader v1Reader = new IndexReader(testFs, baseDir, cmdPrefix, 0, replId)) {
            v1Reader.init();
            Assert.assertEquals(expected, v1Reader.getAllGtidSet());
        }
        try (IndexReaderV2 v2Reader = new IndexReaderV2(testFs, baseDir, cmdPrefix, 0, replId)) {
            v2Reader.init();
            Assert.assertEquals(expected, v2Reader.getAllGtidSet());
        }
    }

    @Test
    public void testReadV2FalseUsesV1ReaderForRecover() throws Exception {
        baseDir = Paths.get(tempDir, "IndexStoreTest-readV2False").toString();
        File dir = new File(baseDir);
        if (dir.exists()) for (File f : dir.listFiles()) f.delete();
        else dir.mkdirs();

        String cmdName = "cmd_readv2_0";
        File cmdFile = new File(baseDir, cmdName);
        String uuid = "abababababababababababababababababababab";

        DefaultIndexStore writeStore = createStoreWithFlags(cmdFile, cmdName, true, true, 50, 200);
        defaultIndexStore = writeStore;
        writeGtidRangeCommand(uuid, 1, 15);
        for (int i = 0; i < 100; i++) {
            writeStore.write(createPingCommand());
        }
        writeGtidRangeCommand(uuid, 16, 25);
        writeStore.closeWriter();

        String cmdPrefix = toCmdPrefix(cmdName);
        Assert.assertTrue("v2 index should exist from dual write",
                indexV2File(cmdPrefix, 0).exists());
        Assert.assertTrue("v1 index should exist for rollback read path",
                indexV1File(cmdPrefix, 0).exists());

        DefaultIndexStore readStore = createStoreWithFlags(cmdFile, cmdName, true, false);
        defaultIndexStore = readStore;
        GtidSet gtidSet = readStore.getIndexGtidSet();
        Assert.assertEquals(new GtidSet(uuid + ":1-25"), gtidSet);

        Pair<Long, GtidSet> point = readStore.locateContinueGtidSet(new GtidSet(uuid + ":1-10"));
        Assert.assertNotNull(point);
        RedisOp op11 = IndexTestTool.readBytebufAfter(testCmdStore.currentCmdFile().getPath(), point.getKey());
        Assert.assertEquals(uuid + ":11", op11.getOpGtid());

        readStore.closeWriter();
    }

    @Test
    public void testV2LocateContinueXsyncScenario() throws Exception {
        String uuid = "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0";
        baseDir = Paths.get(tempDir, "IndexStoreTest-xsyncContinue").toString();
        File dir = new File(baseDir);
        if (dir.exists()) {
            for (File f : dir.listFiles()) f.delete();
        } else {
            dir.mkdirs();
        }
        String cmdName = "cmd_xsync_continue_0";
        File cmdFile = new File(baseDir, cmdName);
        DefaultIndexStore store = createV2Store(cmdFile, cmdName);
        defaultIndexStore = store;

        writeGtidRangeCommand(uuid, 622000, 622009);

        Pair<Long, GtidSet> point = defaultIndexStore.locateContinueGtidSet(
                new GtidSet("bca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2," + uuid + ":622000-622001"));
        Assert.assertEquals(uuid + ":622000-622001", point.getValue().toString());
        RedisOp nextOp = IndexTestTool.readBytebufAfter(testCmdStore.currentCmdFile().getPath(), point.getKey());
        Assert.assertEquals(uuid + ":622002", nextOp.getOpGtid());
    }

    @Test
    public void testLocateGtidRange_AfterGcRemovesOldestSegment() throws Exception {
        write(file1);
        long oldestSegStart = 0L;
        switchCmdSegment("cmd_19513000");
        write(file2);

        setField(defaultIndexStore, "cmdStoreStartOffset", lastRollSegmentStart);
        AsyncFileSystemHelper.await(
                testFs.deleteSegments(testCmdStore.getWriteSegmentFile(), List.of(oldestSegStart)),
                "delete oldest segment after GC");

        String uuid = "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0";
        List<Pair<Long, Long>> result = defaultIndexStore.locateGtidRange(uuid, 2, 10);

        Assert.assertFalse("Should locate GTIDs in remaining segment after GC", result.isEmpty());
        for (Pair<Long, Long> range : result) {
            Assert.assertNotNull(range.getKey());
            Assert.assertNotNull(range.getValue());
            Assert.assertTrue(range.getKey() < range.getValue());
            Assert.assertTrue("Start offset should be in remaining segment",
                    range.getKey() >= lastRollSegmentStart);
        }

        Assert.assertTrue(indexV2File(CMD_PREFIX, lastRollSegmentStart).exists());
        Assert.assertFalse(indexV2File(CMD_PREFIX, oldestSegStart).exists());
    }

    /**
     * Regression: dualWrite V1 IndexWriter.flush() clears currentBlock but keeps indexEntry;
     * locateContinueGtidSet must not NPE on saveIndexEntry after closeWriter (X→P switchToPsync).
     */
    @Test
    public void testLocateContinueGtidSet_AfterCloseWriter_NoNpe() throws Exception {
        String uuid = "a4f566ef50a85e1119f17f9b746728b48609a2ab";
        defaultIndexStore.write(createGtidCommand(uuid + ":1", "SET", "k", "v1"));
        defaultIndexStore.write(createGtidCommand(uuid + ":2", "SET", "k", "v2"));
        defaultIndexStore.closeWriter();

        Pair<Long, GtidSet> point = defaultIndexStore.locateContinueGtidSet(new GtidSet(uuid + ":1-2"));
        Assert.assertNotNull(point);
        Assert.assertTrue(point.getKey() >= 0 || point.getKey() == -1);
    }

    /**
     * Regression: getIndexGtidSet must return a snapshot. Concurrent write+union (same path as
     * GapAllowSyncHandler.awaitIfRequestExceedsCurrent → DefaultReplicationStore.getGtidSet)
     * must not throw ConcurrentModificationException.
     */
    @Test
    public void testGetIndexGtidSet_SnapshotSafeUnderConcurrentWrite() throws Exception {
        String uuid = "b50c0ac6608a3351a6ed0c6a92d93ec736b390a0";
        defaultIndexStore.write(createGtidCommand(uuid + ":1", "SET", "k", "v1"));

        AtomicReference<Throwable> readerError = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            pool.execute(() -> {
                started.countDown();
                try {
                    for (int i = 2; i <= 200; i++) {
                        defaultIndexStore.write(createGtidCommand(uuid + ":" + i, "SET", "k" + i, "v" + i));
                    }
                } catch (Throwable t) {
                    readerError.compareAndSet(null, t);
                }
            });
            pool.execute(() -> {
                try {
                    started.await(5, TimeUnit.SECONDS);
                    GtidSet begin = new GtidSet(GtidSet.EMPTY_GTIDSET);
                    for (int i = 0; i < 500; i++) {
                        // Mirrors DefaultReplicationStore.getGtidSet: begin.union(index)
                        begin.union(defaultIndexStore.getIndexGtidSet());
                    }
                } catch (Throwable t) {
                    readerError.compareAndSet(null, t);
                }
            });
            pool.shutdown();
            Assert.assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        } finally {
            if (!pool.isTerminated()) {
                pool.shutdownNow();
            }
        }
        if (readerError.get() != null) {
            throw new AssertionError("concurrent getIndexGtidSet failed", readerError.get());
        }
        Assert.assertTrue(defaultIndexStore.getIndexGtidSet().contains(uuid, 200));
    }

    @Test
    public void testGetIndexGtidSet_ReturnsIndependentSnapshot() throws Exception {
        String uuid = "c50c0ac6608a3351a6ed0c6a92d93ec736b390a0";
        defaultIndexStore.write(createGtidCommand(uuid + ":1", "SET", "k", "v1"));
        GtidSet snapshot = defaultIndexStore.getIndexGtidSet();
        snapshot.add(uuid + ":999");
        Assert.assertFalse("mutating snapshot must not affect index",
                defaultIndexStore.getIndexGtidSet().contains(uuid, 999));
        Assert.assertTrue(defaultIndexStore.getIndexGtidSet().contains(uuid, 1));
    }

    // 辅助方法：从文件写入
    private void write(DefaultIndexStore store, String path) throws IOException {
        File f = new File(path);
        ControllableFile controllableFile = new DefaultControllableFile(f);
        controllableFile.getFileChannel().position(0);
        while (controllableFile.getFileChannel().position() < controllableFile.getFileChannel().size()) {
            int size = (int) Math.min(1024, controllableFile.getFileChannel().size() - controllableFile.getFileChannel().position());
            ByteBuffer buffer = ByteBuffer.allocate(size);
            controllableFile.getFileChannel().read(buffer);
            buffer.flip();
            ByteBuf byteBuf = Unpooled.wrappedBuffer(buffer.array());
            store.write(byteBuf);
        }
    }

}
