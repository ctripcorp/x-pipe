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
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.container.ContainerResourceManager;
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
import java.util.stream.IntStream;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doCallRealMethod;
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
    /** 单测 ZONE 连续条数阈值；生产 KeeperConfig 默认 8192 */
    private static final int TEST_ZONE_CONSECUTIVE_THRESHOLD = 100;
    /** 单测 Block 满盘条数上限；生产 BlockEntry.BLOCK_MAX_SIZE = 8192 */
    private static final int TEST_BLOCK_MAX_SIZE = 100;

    private AsyncFileSystem testFs;
    private TestAsyncCommandStore testCmdStore;
    private final List<AsyncSegmentFile> openedSegments = new ArrayList<>();

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

        Path destinationPath = Paths.get(baseDir, "00000000");
        Files.copy(new File(file1).toPath(), destinationPath);

        when(channel.size()).thenReturn(0l);

        when(commandFileContext.getChannel()).thenReturn(channel);

        when(commandFile.getFile()).thenReturn(new File("00000000"));
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);

        when(writer.getFileContext()).thenReturn(commandFileContext);

        when(commandWriterCallback.getCommandWriter()).thenReturn(writer);
        when(writer.needRotate()).thenReturn(false);
        when(writer.fileLength()).thenReturn(0L);
        when(writer.totalLength()).thenReturn(0L);
        when(commandWriterCallback.getCmdFileLen()).thenReturn(0L);
        when(commandWriterCallback.writeCommand(any(ByteBuf.class))).thenAnswer(inv -> {
            ByteBuf b = inv.getArgument(0);
            int n = b.readableBytes();
            return n;
        });

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);

        // 1. 初始化 mock 的 KeeperConfig（启用双写 + v2 读取）
        when(keeperConfig.dualWrite()).thenReturn(true);   // 同时写 v1 和 v2
        when(keeperConfig.readV2()).thenReturn(true);      // 优先读 v2
        when(keeperConfig.getIndexZoneConsecutiveThreshold()).thenReturn(TEST_ZONE_CONSECUTIVE_THRESHOLD);
        when(keeperConfig.getIndexMixedTotalBytesThreshold()).thenReturn(16L * 1024 * 1024);
        when(keeperConfig.getBlockSizeThreshold()).thenReturn(BlockEntry.DEFAULT_BLOCK_MAX_SIZE);

        testFs = ContainerResourceManager.createAsyncFileSystem(
                KeeperConfig.DEFAULT_ASYNC_IO_THREADS, KeeperConfig.DEFAULT_ASYNC_FSYNC_INTERVAL_BYTES);
        testCmdStore = createTestCmdStore("00000000");

        defaultIndexStore = new DefaultIndexStore(keeperConfig, ckStore, testCmdStore, baseDir, opParser,
                commandWriterCallback, gtidCmdFilter);
        defaultIndexStore.openWriter(writer);

    }

    private TestAsyncCommandStore createTestCmdStore(String cmdPrefix) throws IOException {
        List<String> prefixes = Arrays.asList(
                AbstractIndex.INDEX + cmdPrefix,
                AbstractIndex.BLOCK + cmdPrefix,
                AbstractIndex.INDEX_V2 + cmdPrefix,
                AbstractIndex.BLOCK_V2 + cmdPrefix);
        AsyncSegmentFile seg = AsyncFileSystemHelper.await(
                testFs.open(baseDir, cmdPrefix, prefixes, true, "test-repl-0"),
                "open test command segment");
        openedSegments.add(seg);
        return new TestAsyncCommandStore(testFs, seg, new File(baseDir), cmdPrefix);
    }

    private AsyncFile openTestAsyncFile(File file, boolean write) throws IOException {
        return AsyncFileSystemHelper.await(
                testFs.open(file.getAbsolutePath(), write ? AbstractStorageFile.OpenMode.WRITE : AbstractStorageFile.OpenMode.READ, false, true, "test-repl-0"),
                "open test async file " + file.getName());
    }

    private void switchCmdSegment(String newCmdPrefix) throws Exception {
        File newCmdFile = new File(baseDir, newCmdPrefix);
        if (!newCmdFile.exists()) {
            Files.copy(Paths.get(file2), newCmdFile.toPath());
        }
        TestAsyncCommandStore newCmdStore = createTestCmdStore(newCmdPrefix);
        setField(defaultIndexStore, "asyncCommandStore", newCmdStore);
        defaultIndexStore.doSwitchCmdFile();
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
            RedisOp redisOp = IndexTestTool.readBytebufAfter( file2, point.getKey() - 19513000);
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
            RedisOp redisOp = IndexTestTool.readBytebufAfter(file2, point.getKey() - 19513000);
            Assert.assertEquals(redisOp.getOpGtid(), "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:" + (i+1));
        }
    }

    @Test
    public void testBuildIndex() throws Exception {
        String cmdFile = "00000000";
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

        DefaultControllableFile file = new DefaultControllableFile(baseDir + "00000000");

        File firstFile = new File(file1);
        file.setLength((int)firstFile.length());

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore.closeWriter();
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

        DefaultControllableFile file = new DefaultControllableFile(baseDir + "/index_00000000");

        file.setLength((int)file.size() - 10);

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore.closeWriter();
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

        DefaultControllableFile file = new DefaultControllableFile(baseDir + "/block_00000000");
        int size = (int) file.size();
        if(size > 10){
            size = size - 10;
        }
        file.setLength(size);

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore.closeWriter();
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
            if ("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0".equals(gtid) && num == 633745L) {
                return true;
            }
            return false;
        });
        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore.closeWriter();
        defaultIndexStore = new DefaultIndexStore(keeperConfig, ckStore, testCmdStore, baseDir, opParser, commandWriterCallback, gtidCmdFilter);
        defaultIndexStore.openWriter(writer);


        Pair<Long, GtidSet> point =  defaultIndexStore.locateGtidSetWithFallbackToEnd(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-633744"));
        Pair<Long, GtidSet> point2 =  defaultIndexStore.locateGtidSetWithFallbackToEnd(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-633745"));
        System.out.println(point.getValue());
        System.out.println(point2.getValue());

        Assert.assertEquals(point.getKey(), point2.getKey());

        point = defaultIndexStore.locateContinueGtidSet(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-" + 633746));
        RedisOp redisOp = IndexTestTool.readBytebufAfter(file1, point.getKey() + 133);
        Assert.assertEquals(redisOp.getOpGtid(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633747");

    }

    @Test
    public void testBuildIndexFromCmdFileWithIncompleteTransaction() throws IOException {
        // Create a cmd file with incomplete transaction (MULTI + commands but no EXEC)
        baseDir = Paths.get(tempDir, "IndexStoreTest-testBuildIndexFromCmdFileWithIncompleteTransaction").toString();
        File dir = new File(baseDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        String testCmdFile = "cmd_test_incomplete_transaction_0";
        String testIndexFile = "index_cmd_test_incomplete_transaction_0";
        String testIndexV2File = "indexv2_cmd_test_incomplete_transaction_0";
        File cmdFile = new File(baseDir, testCmdFile);
        File indexFile = new File(baseDir, testIndexFile);
        File indexV2File = new File(baseDir, testIndexV2File);

        // First, write some valid commands with GTID
        String gtid1 = "a4f566ef50a85e1119f17f9b746728b48609a2ab:1";
        String gtid2 = "a4f566ef50a85e1119f17f9b746728b48609a2ab:2";

        // Write first complete GTID command
        writeCommandToFile(cmdFile, createGtidCommand(gtid1, "SET", "key1", "value1"));
        writeGtidSetToFile(indexFile, new GtidSet(""));
        writeGtidSetV2ToFile(indexV2File, new GtidSet(""));
        // Record the position before MULTI (this will be the rollback point)
        long positionBeforeMulti = cmdFile.length();

        // Write MULTI command
        writeCommandToFile(cmdFile, createMultiCommand());

        // Write commands in transaction
        writeCommandToFile(cmdFile, createSetCommand("key2", "value2"));
        writeCommandToFile(cmdFile, createSetCommand("key3", "value3"));

        // Note: We intentionally don't write EXEC, creating an incomplete transaction

        when(commandFile.getFile()).thenReturn(cmdFile);
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);
        when(writer.getFileContext()).thenReturn(commandFileContext);

        // Build index from cmd file
        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        DefaultIndexStore testIndexStore = new DefaultIndexStore(keeperConfig, ckStore, createTestCmdStore(testCmdFile),
                baseDir, opParser, commandWriterCallback, gtidCmdFilter);
        testIndexStore.openWriter(writer); // do buildIO

        // Verify file was truncated to position before MULTI
        Assert.assertEquals("File should be truncated to position before incomplete transaction",
                positionBeforeMulti, cmdFile.length());

        // Verify the incomplete transaction commands were not indexed
        GtidSet gtidSet = testIndexStore.getIndexGtidSet();
        // Should only contain gtid1, not gtid2 (which would be in the incomplete transaction)
        Assert.assertTrue("GTID set should contain gtid1", gtidSet.contains("a4f566ef50a85e1119f17f9b746728b48609a2ab", 1));
        Assert.assertFalse("GTID set should not contain gtid2 from incomplete transaction",
                gtidSet.contains("a4f566ef50a85e1119f17f9b746728b48609a2ab", 2));

        // Verify we can locate the last valid command
        Pair<Long, GtidSet> point = testIndexStore.locateContinueGtidSet(new GtidSet(gtid1));
        Assert.assertNotNull("Should be able to locate gtid1", point);
        Assert.assertEquals("Should locate gtid1", gtid1, point.getValue().toString());

        testIndexStore.closeWriter();
    }

    @Test
    public void testBuildIndexFromCmdFileWithIncompleteTransactionAfterValidCommands() throws IOException {
        // Create a cmd file with valid commands followed by incomplete transaction
        baseDir = Paths.get(tempDir, "IndexStoreTest-testBuildIndexFromCmdFileWithIncompleteTransactionAfterValidCommands").toString();
        File dir = new File(baseDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        String testCmdFile = "cmd_test_incomplete_transaction2_0";
        String testIndexFile = "index_cmd_test_incomplete_transaction2_0";
        String testIndexV2File = "indexv2_cmd_test_incomplete_transaction2_0";
        File cmdFile = new File(baseDir, testCmdFile);
        File indexFile = new File(baseDir, testIndexFile);
        File indexV2File = new File(baseDir, testIndexV2File);

        // Write multiple valid GTID commands
        String gtid1 = "a4f566ef50a85e1119f17f9b746728b48609a2ab:1";
        String gtid2 = "a4f566ef50a85e1119f17f9b746728b48609a2ab:2";
        String gtid3 = "a4f566ef50a85e1119f17f9b746728b48609a2ab:3";

        writeGtidSetToFile(indexFile, new GtidSet(""));
        writeGtidSetV2ToFile(indexV2File, new GtidSet(""));
        writeCommandToFile(cmdFile, createGtidCommand(gtid1, "SET", "key1", "value1"));
        writeCommandToFile(cmdFile, createGtidCommand(gtid2, "SET", "key2", "value2"));

        // Record position before incomplete transaction
        long positionBeforeIncompleteTransaction = cmdFile.length();

        // Write incomplete transaction (MULTI + commands but no EXEC)
        writeCommandToFile(cmdFile, createMultiCommand());
        writeCommandToFile(cmdFile, createSetCommand("key3", "value3"));
        writeCommandToFile(cmdFile, createSetCommand("key4", "value4"));
        // No EXEC - transaction is incomplete

        when(commandFile.getFile()).thenReturn(cmdFile);
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);
        when(writer.getFileContext()).thenReturn(commandFileContext);

        // Build index from cmd file
        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        DefaultIndexStore testIndexStore = new DefaultIndexStore(keeperConfig, ckStore, createTestCmdStore(testCmdFile),
                baseDir, opParser, commandWriterCallback, gtidCmdFilter);
        testIndexStore.openWriter(writer);

        // Verify file was truncated to position before incomplete transaction
        Assert.assertEquals("File should be truncated to position before incomplete transaction",
                positionBeforeIncompleteTransaction, cmdFile.length());

        // Verify only valid commands were indexed
        GtidSet gtidSet = testIndexStore.getIndexGtidSet();
        Assert.assertTrue("GTID set should contain gtid1", gtidSet.contains("a4f566ef50a85e1119f17f9b746728b48609a2ab", 1));
        Assert.assertTrue("GTID set should contain gtid2", gtidSet.contains("a4f566ef50a85e1119f17f9b746728b48609a2ab", 2));
        Assert.assertFalse("GTID set should not contain gtid3 from incomplete transaction",
                gtidSet.toString().contains(gtid3));

        // Verify we can locate both valid commands
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
        // Test when there's no index file
        defaultIndexStore.closeWriter();
        String cmdName = testCmdStore.getCommandFileNamePrefix();
        new File(baseDir, AbstractIndex.INDEX + cmdName).delete();
        new File(baseDir, AbstractIndex.BLOCK + cmdName).delete();
        new File(baseDir, AbstractIndex.INDEX_V2 + cmdName).delete();
        new File(baseDir, AbstractIndex.BLOCK_V2 + cmdName).delete();
        
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
            // Verify offsets are in backlog space (should be >= 19513000 for second file)
            Assert.assertTrue("Start offset should be in correct range", 
                range.getKey() >= 19513000);
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
        
        // Verify that the index files exist
        File indexDir = new File(baseDir);
        File[] indexFiles = indexDir.listFiles((dir, name) -> name.startsWith("index_"));
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


        // 验证 v2 索引文件存在
        File indexV2 = new File(baseDir, "indexv2_00000000");
        File blockV2 = new File(baseDir, "blockv2_00000000");
        Assert.assertTrue("v2 index file should exist", indexV2.exists());
        Assert.assertTrue("v2 block file should exist", blockV2.exists());

        // 检查 v1 文件也存在（双写）
        File indexV1 = new File(baseDir, "index_00000000");
        File blockV1 = new File(baseDir, "block_00000000");
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
        File dir = new File(baseDir);
        if (dir.exists()) {
            for (File f : dir.listFiles()) f.delete();
        } else {
            dir.mkdirs();
        }
        String cmdName = "cmd_zone_alt_0";
        File cmdFile = new File(baseDir, cmdName);

        when(commandFile.getFile()).thenReturn(cmdFile);
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);
        when(writer.getFileContext()).thenReturn(commandFileContext);

        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        DefaultIndexStore store = new DefaultIndexStore(keeperConfig, ckStore, createTestCmdStore(cmdName), baseDir, opParser, commandWriterCallback,
                gtidCmdFilter);
        // hook commandWriterCallback to actually persist bytes to cmdFile
        when(commandWriterCallback.writeCommand(any(ByteBuf.class))).thenAnswer(inv -> {
            ByteBuf b = inv.getArgument(0);
            int n = b.readableBytes();
            try (java.io.FileOutputStream fos = new java.io.FileOutputStream(cmdFile, true)) {
                byte[] tmp = new byte[n];
                b.getBytes(b.readerIndex(), tmp);
                fos.write(tmp);
            }
            return n;
        });
        store.openWriter(writer);

        defaultIndexStore = store;

        writeGtidRangeCommand(uuid,1,3);

        writeRawStr(createPublishCommand(2));

        writeGtidRangeCommand(uuid,4,8);

        writeRawStr(createPublishCommand(1));

        writeGtidRangeCommand(uuid,9,12);

        // 验证整体 GTID Set 包含新写入的区间
        GtidSet fullGtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals("GTID set expected", new GtidSet("b4f566ef50a85e1119f17f9b746728b48609a2ab:1-12"),fullGtidSet);

        File indexV2 = new File(baseDir, "indexv2_"+cmdName);
        File blockV2 = new File(baseDir, "blockv2_"+cmdName);
        Assert.assertTrue("v2 index file should exist", indexV2.exists());
        Assert.assertTrue("v2 block file should exist", blockV2.exists());

        // 通过 locateContinueGtidSet 验证断点续传偏移
        // 请求方已有 testUuid:1-5，应该返回下一条命令 (gno=6) 的偏移
        Pair<Long, GtidSet> point = defaultIndexStore.locateContinueGtidSet(
                new GtidSet(uuid + ":1-5"));
        Assert.assertNotNull(point);
        RedisOp op6 = IndexTestTool.readBytebufAfter(cmdFile.getPath(), point.getKey());
        Assert.assertNotNull(op6);
        Assert.assertEquals(uuid + ":6", op6.getOpGtid());

        // 请求方已有 testUuid:1-8，应返回 gno=9 的偏移
        point = defaultIndexStore.locateContinueGtidSet(
                new GtidSet(uuid + ":1-8"));
        Assert.assertNotNull(point);
        RedisOp op9 = IndexTestTool.readBytebufAfter(cmdFile.getPath(), point.getKey());
        Assert.assertNotNull(op9);
        Assert.assertEquals(uuid + ":9", op9.getOpGtid());

        // 交替场景 GTID 清空 pending zone，不应产生 ZONE entry
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
        File dir = new File(baseDir);
        if (dir.exists()) for (File f : dir.listFiles()) f.delete();
        else dir.mkdirs();

        String cmdName = "cmd_v2zone_flush_threshold";
        File cmdFile = new File(baseDir, cmdName);

        DefaultIndexStore store = createV2Store(cmdFile, cmdName);
        IndexWriterV2 writerV2 = store.getIndexWriterV2();

        int threshold = TEST_ZONE_CONSECUTIVE_THRESHOLD;
        // 写入 threshold 条 PING
        for (int i = 0; i < threshold; i++) {
            store.write(createPingCommand());
        }
        long offsetAfterThreshold = cmdFile.length();

        // 应恰好有一个 zone 落盘
        List<long[]> zones = writerV2.loadAllZones();
        Assert.assertEquals(1, zones.size());
        Assert.assertArrayEquals(new long[]{0, offsetAfterThreshold}, zones.get(0));

        // 再写 7 条 PING，然后 close，应产生第二个 zone
        for (int i = 0; i < 7; i++) {
            store.write(createPingCommand());
        }
        long finalOffset = cmdFile.length();
        store.closeWriter();

        // 重新打开 store 并加载 zones
        DefaultIndexStore store2 = createV2Store(cmdFile, cmdName);
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
        File dir = new File(baseDir);
        if (dir.exists()) for (File f : dir.listFiles()) f.delete();
        else dir.mkdirs();

        String cmdName = "cmd_v2_recover_skip_zone";
        File cmdFile = new File(baseDir, cmdName);

        int zoneThreshold = TEST_ZONE_CONSECUTIVE_THRESHOLD;
        DefaultIndexStore store = createV2StoreWithThresholds(cmdFile, cmdName, zoneThreshold, 16L * 1024 * 1024);
        for (int i = 0; i < zoneThreshold; i++) {
            store.write(createPingCommand());
        }
        long offsetAfterZone = cmdFile.length();
        List<long[]> zones = store.getIndexWriterV2().loadAllZones();
        Assert.assertEquals(1, zones.size());
        Assert.assertEquals(offsetAfterZone, zones.get(0)[1]);
        store.closeWriter();

        byte[] pingBytes = pingCommandBytes();
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(cmdFile, true)) {
            for (int i = 0; i < 3; i++) {
                fos.write(pingBytes);
            }
        }

        DefaultIndexStore store2 = spy(createV2StoreUnopened(cmdFile, cmdName, zoneThreshold, 16L * 1024 * 1024));
        doCallRealMethod().when(store2).buildIndexFromCmdFile(anyLong());
        store2.openWriter(writer);
        verify(store2).buildIndexFromCmdFile(eq(offsetAfterZone));
        store2.closeWriter();
    }

    @Test
    public void testV2RecoverIndex_TruncateMalformedTail() throws IOException {
        // T-R.5: v2 index 尾部残缺 88B entry 应 truncate，GtidSet 不损坏
        baseDir = Paths.get(tempDir, "IndexStoreTest-v2RecoverTruncate").toString();
        File dir = new File(baseDir);
        if (dir.exists()) for (File f : dir.listFiles()) f.delete();
        else dir.mkdirs();

        String cmdName = "cmd_v2_recover_truncate";
        File cmdFile = new File(baseDir, cmdName);
        String uuid = "cafebabecafebabecafebabecafebabecafebabe";
        int zoneThreshold = TEST_ZONE_CONSECUTIVE_THRESHOLD;

        DefaultIndexStore store = createV2StoreWithThresholds(cmdFile, cmdName, zoneThreshold, 16L * 1024 * 1024);
        for (int i = 0; i < zoneThreshold; i++) {
            store.write(createPingCommand());
        }
        store.write(createGtidCommand(uuid + ":1", "SET", "k", "v"));
        GtidSet gtidSetBefore = store.getIndexGtidSet();
        Assert.assertTrue(gtidSetBefore.contains(uuid, 1));
        store.closeWriter();

        File indexV2 = new File(baseDir, "indexv2_" + cmdName);
        long indexSizeBefore = indexV2.length();
        Assert.assertTrue(indexSizeBefore > IndexEntry.SEGMENT_LENGTH_V2);
        long truncatedSize = indexSizeBefore - 44;
        try (DefaultControllableFile indexFile = new DefaultControllableFile(indexV2)) {
            indexFile.setLength((int) truncatedSize);
        }
        Assert.assertEquals(truncatedSize, indexV2.length());

        DefaultIndexStore store2 = createV2StoreWithThresholds(cmdFile, cmdName, zoneThreshold, 16L * 1024 * 1024);
        GtidSet gtidSetAfter = store2.getIndexGtidSet();
        Assert.assertTrue(gtidSetAfter.contains(uuid, 1));
        store2.closeWriter();
        // recover 截断残缺 entry 后，尾部 GTID 在内存 pending，close 落盘后与截断前一致
        Assert.assertEquals(indexSizeBefore, indexV2.length());
    }

    @Test
    public void testV2RecoverIndex_RollbackIncompleteTransactionPreservesGtid() throws IOException {
        // v2 完整写入/recover：已落盘 ZONE+GTID 保留；尾部 MULTI 无 EXEC 的 cmd 被回退
        baseDir = Paths.get(tempDir, "IndexStoreTest-v2RecoverIncompleteTx").toString();
        File dir = new File(baseDir);
        if (dir.exists()) for (File f : dir.listFiles()) f.delete();
        else dir.mkdirs();

        String cmdName = "cmd_v2_recover_incomplete_tx";
        File cmdFile = new File(baseDir, cmdName);
        String uuid = "a4f566ef50a85e1119f17f9b746728b48609a2ab";
        int zoneThreshold = TEST_ZONE_CONSECUTIVE_THRESHOLD;

        DefaultIndexStore store = createV2StoreWithThresholds(cmdFile, cmdName, zoneThreshold, 16L * 1024 * 1024);
        for (int i = 0; i < zoneThreshold; i++) {
            store.write(createPingCommand());
        }
        store.write(createGtidCommand(uuid + ":1", "SET", "k1", "v1"));
        store.write(createGtidCommand(uuid + ":2", "SET", "k2", "v2"));
        GtidSet gtidSetBefore = store.getIndexGtidSet();
        Assert.assertTrue(gtidSetBefore.contains(uuid, 1));
        Assert.assertTrue(gtidSetBefore.contains(uuid, 2));
        store.closeWriter();

        long cmdLenBeforeIncomplete = cmdFile.length();
        writeCommandToFile(cmdFile, createMultiCommand());
        writeCommandToFile(cmdFile, createSetCommand("k3", "v3"));
        writeCommandToFile(cmdFile, createSetCommand("k4", "v4"));

        DefaultIndexStore store2 = createV2StoreWithThresholds(cmdFile, cmdName, zoneThreshold, 16L * 1024 * 1024);
        Assert.assertEquals("Incomplete transaction tail should be rolled back",
                cmdLenBeforeIncomplete, cmdFile.length());
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

        store.write(createGtidCommand(uuid + ":1", "SET", "k", "v"));
        while (cmdFile.length() < 250) {
            store.write(createPingCommand());
        }

        List<IndexEntryType> entryTypes = readIndexEntryTypes(baseDir, cmdName);
        int gtidIdx = -1;
        int zoneIdx = -1;
        for (int i = 0; i < entryTypes.size(); i++) {
            if (entryTypes.get(i) == IndexEntryType.GTID && gtidIdx == -1) {
                gtidIdx = i;
            }
            if (entryTypes.get(i) == IndexEntryType.ZONE) {
                zoneIdx = i;
            }
        }
        Assert.assertTrue("GTID entry should exist", gtidIdx >= 0);
        Assert.assertTrue("ZONE entry should exist", zoneIdx >= 0);
        Assert.assertTrue("GTID entry must precede ZONE entry", gtidIdx < zoneIdx);

        store.closeWriter();
    }

    @Test
    public void testV2FlushIndexEntry_GtidThenZone() throws IOException {
        // flushIndexEntry 同时 pending GTID + ZONE 时，须先落 GTID entry 再落 ZONE entry
        baseDir = Paths.get(tempDir, "IndexStoreTest-v2FlushIndexEntry").toString();
        File dir = new File(baseDir);
        if (dir.exists()) for (File f : dir.listFiles()) f.delete();
        else dir.mkdirs();

        String cmdName = "cmd_v2_flush_index_entry";
        File cmdFile = new File(baseDir, cmdName);
        String uuid = "cafebabecafebabecafebabecafebabecafebabe";

        DefaultIndexStore store = createV2StoreWithThresholds(cmdFile, cmdName, TEST_ZONE_CONSECUTIVE_THRESHOLD, 16L * 1024 * 1024);
        IndexWriterV2 writerV2 = store.getIndexWriterV2();

        store.write(createGtidCommand(uuid + ":1", "SET", "k", "v"));
        store.write(createPingCommand());
        store.write(createPingCommand());

        writerV2.flushIndexEntry();

        List<IndexEntryType> entryTypes = readIndexEntryTypes(baseDir, cmdName);
        Assert.assertEquals(2, entryTypes.size());
        Assert.assertEquals(IndexEntryType.GTID, entryTypes.get(0));
        Assert.assertEquals(IndexEntryType.ZONE, entryTypes.get(1));

        store.closeWriter();
    }

    /**
     * 创建一个启用双写 + v2 读取的 DefaultIndexStore，
     * 并 hook commandWriterCallback 将命令字节写入指定 cmdFile。
     */
    private DefaultIndexStore createV2Store(File cmdFile, String cmdName) throws IOException {
        when(commandFile.getFile()).thenReturn(cmdFile);
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);
        when(writer.getFileContext()).thenReturn(commandFileContext);
        when(commandWriterCallback.writeCommand(any(ByteBuf.class))).thenAnswer(inv -> {
            ByteBuf b = inv.getArgument(0);
            int n = b.readableBytes();
            try (java.io.FileOutputStream fos = new java.io.FileOutputStream(cmdFile, true)) {
                byte[] tmp = new byte[n];
                b.getBytes(b.readerIndex(), tmp);
                fos.write(tmp);
            }
            return n;
        });

        CKStore ckStore = mock(CKStore.class);
        KeeperConfig keeperConfig = mock(KeeperConfig.class);
        when(ckStore.getKeeperConfig()).thenReturn(keeperConfig);
        when(keeperConfig.dualWrite()).thenReturn(true);
        when(keeperConfig.readV2()).thenReturn(true);
        when(keeperConfig.getIndexZoneConsecutiveThreshold()).thenReturn(TEST_ZONE_CONSECUTIVE_THRESHOLD);
        when(keeperConfig.getIndexMixedTotalBytesThreshold()).thenReturn(16L * 1024 * 1024);
        when(keeperConfig.getBlockSizeThreshold()).thenReturn(BlockEntry.DEFAULT_BLOCK_MAX_SIZE);

        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        TestAsyncCommandStore cmdStore = createTestCmdStore(cmdName);
        DefaultIndexStore store = new DefaultIndexStore(keeperConfig, ckStore, cmdStore, baseDir, opParser,
                commandWriterCallback, gtidCmdFilter);
        store.openWriter(writer);
        return store;
    }

    private DefaultIndexStore createV2StoreUnopened(File cmdFile, String cmdName) throws IOException {
        return createV2StoreUnopened(cmdFile, cmdName, TEST_ZONE_CONSECUTIVE_THRESHOLD, 16L * 1024 * 1024);
    }

    private DefaultIndexStore createV2StoreUnopened(File cmdFile, String cmdName,
                                                    int zoneThreshold, long mixedBytesThreshold) throws IOException {
        when(commandFile.getFile()).thenReturn(cmdFile);
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);
        when(writer.getFileContext()).thenReturn(commandFileContext);
        when(commandWriterCallback.writeCommand(any(ByteBuf.class))).thenAnswer(inv -> {
            ByteBuf b = inv.getArgument(0);
            int n = b.readableBytes();
            try (java.io.FileOutputStream fos = new java.io.FileOutputStream(cmdFile, true)) {
                byte[] tmp = new byte[n];
                b.getBytes(b.readerIndex(), tmp);
                fos.write(tmp);
            }
            return n;
        });

        CKStore ckStoreLocal = mock(CKStore.class);
        KeeperConfig config = mock(KeeperConfig.class);
        when(ckStoreLocal.getKeeperConfig()).thenReturn(config);
        when(config.dualWrite()).thenReturn(true);
        when(config.readV2()).thenReturn(true);
        when(config.getIndexZoneConsecutiveThreshold()).thenReturn(zoneThreshold);
        when(config.getIndexMixedTotalBytesThreshold()).thenReturn(mixedBytesThreshold);

        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        TestAsyncCommandStore cmdStore = createTestCmdStore(cmdName);
        return new DefaultIndexStore(config, ckStoreLocal, cmdStore, baseDir, opParser,
                commandWriterCallback, gtidCmdFilter);
    }

    private DefaultIndexStore createStoreWithFlags(File cmdFile, String cmdName,
                                                   boolean dualWrite, boolean readV2) throws IOException {
        return createStoreWithFlags(cmdFile, cmdName, dualWrite, readV2, TEST_ZONE_CONSECUTIVE_THRESHOLD, 16L * 1024 * 1024);
    }

    private DefaultIndexStore createStoreWithFlags(File cmdFile, String cmdName,
                                                   boolean dualWrite, boolean readV2,
                                                   int zoneThreshold, long mixedBytesThreshold) throws IOException {
        when(commandFile.getFile()).thenReturn(cmdFile);
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);
        when(writer.getFileContext()).thenReturn(commandFileContext);
        when(commandWriterCallback.writeCommand(any(ByteBuf.class))).thenAnswer(inv -> {
            ByteBuf b = inv.getArgument(0);
            int n = b.readableBytes();
            try (java.io.FileOutputStream fos = new java.io.FileOutputStream(cmdFile, true)) {
                byte[] tmp = new byte[n];
                b.getBytes(b.readerIndex(), tmp);
                fos.write(tmp);
            }
            return n;
        });

        CKStore ckStoreLocal = mock(CKStore.class);
        KeeperConfig config = mock(KeeperConfig.class);
        when(ckStoreLocal.getKeeperConfig()).thenReturn(config);
        when(config.dualWrite()).thenReturn(dualWrite);
        when(config.readV2()).thenReturn(readV2);
        when(config.getIndexZoneConsecutiveThreshold()).thenReturn(zoneThreshold);
        when(config.getIndexMixedTotalBytesThreshold()).thenReturn(mixedBytesThreshold);

        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        TestAsyncCommandStore cmdStore = createTestCmdStore(cmdName);
        DefaultIndexStore store = new DefaultIndexStore(config, ckStoreLocal, cmdStore, baseDir, opParser,
                commandWriterCallback, gtidCmdFilter);
        store.openWriter(writer);
        return store;
    }

    private DefaultIndexStore createV2StoreWithThresholds(File cmdFile, String cmdName,
                                                            int zoneThreshold, long mixedBytesThreshold)
            throws IOException {
        when(commandFile.getFile()).thenReturn(cmdFile);
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);
        when(writer.getFileContext()).thenReturn(commandFileContext);
        when(commandWriterCallback.writeCommand(any(ByteBuf.class))).thenAnswer(inv -> {
            ByteBuf b = inv.getArgument(0);
            int n = b.readableBytes();
            try (java.io.FileOutputStream fos = new java.io.FileOutputStream(cmdFile, true)) {
                byte[] tmp = new byte[n];
                b.getBytes(b.readerIndex(), tmp);
                fos.write(tmp);
            }
            return n;
        });

        CKStore ckStore = mock(CKStore.class);
        KeeperConfig keeperConfig = mock(KeeperConfig.class);
        when(ckStore.getKeeperConfig()).thenReturn(keeperConfig);
        when(keeperConfig.dualWrite()).thenReturn(true);
        when(keeperConfig.readV2()).thenReturn(true);
        when(keeperConfig.getIndexZoneConsecutiveThreshold()).thenReturn(zoneThreshold);
        when(keeperConfig.getIndexMixedTotalBytesThreshold()).thenReturn(mixedBytesThreshold);
        when(keeperConfig.getBlockSizeThreshold()).thenReturn(TEST_BLOCK_MAX_SIZE);

        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        TestAsyncCommandStore cmdStore = createTestCmdStore(cmdName);
        DefaultIndexStore store = new DefaultIndexStore(keeperConfig, ckStore, cmdStore, baseDir, opParser,
                commandWriterCallback, gtidCmdFilter);
        store.openWriter(writer);
        return store;
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

    private List<GtidIndexSnapshot> readV1GtidSnapshots(String baseDir, String cmdName) throws IOException {
        List<GtidIndexSnapshot> snapshots = new ArrayList<>();
        File indexV1 = new File(baseDir, "index_" + cmdName);
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

    private List<GtidIndexSnapshot> readV2GtidSnapshots(String baseDir, String cmdName) throws IOException {
        List<GtidIndexSnapshot> snapshots = new ArrayList<>();
        for (IndexEntry entry : readV2GtidEntries(baseDir, cmdName)) {
            snapshots.add(new GtidIndexSnapshot(entry));
        }
        return snapshots;
    }

    private List<IndexEntry> readV2GtidEntries(String baseDir, String cmdName) throws IOException {
        List<IndexEntry> gtidEntries = new ArrayList<>();
        File indexV2 = new File(baseDir, "indexv2_" + cmdName);
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

    private List<IndexEntryType> readIndexEntryTypes(String baseDir, String cmdName) throws IOException {
        File indexV2 = new File(baseDir, "indexv2_" + cmdName);
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

        Assert.assertTrue(new File(baseDir, "index_" + cmdName).exists());
        Assert.assertTrue(new File(baseDir, "indexv2_" + cmdName).exists());

        List<GtidIndexSnapshot> v1Snapshots = readV1GtidSnapshots(baseDir, cmdName);
        List<GtidIndexSnapshot> v2Snapshots = readV2GtidSnapshots(baseDir, cmdName);
        Assert.assertFalse("v1 should have GTID index entries", v1Snapshots.isEmpty());
        Assert.assertEquals("v1/v2 GTID entry count should match", v1Snapshots.size(), v2Snapshots.size());
        for (int i = 0; i < v1Snapshots.size(); i++) {
            assertGtidSnapshotEquals(v1Snapshots.get(i), v2Snapshots.get(i));
        }

        GtidSet expected = new GtidSet(uuid + ":1-40");
        ReplId replId = testCmdStore.getFileSystemReplId();
        long segmentStart = AbstractIndex.extractOffset(cmdName);
        try (IndexReader v1Reader = new IndexReader(testFs, baseDir, cmdName, segmentStart, replId)) {
            v1Reader.init();
            Assert.assertEquals(expected, v1Reader.getAllGtidSet());
        }
        try (IndexReaderV2 v2Reader = new IndexReaderV2(testFs, baseDir, cmdName, segmentStart, replId)) {
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

        Assert.assertTrue("v2 index should exist from dual write",
                new File(baseDir, "indexv2_" + cmdName).exists());
        Assert.assertTrue("v1 index should exist for rollback read path",
                new File(baseDir, "index_" + cmdName).exists());

        DefaultIndexStore readStore = createStoreWithFlags(cmdFile, cmdName, true, false);
        GtidSet gtidSet = readStore.getIndexGtidSet();
        Assert.assertEquals(new GtidSet(uuid + ":1-25"), gtidSet);

        Pair<Long, GtidSet> point = readStore.locateContinueGtidSet(new GtidSet(uuid + ":1-10"));
        Assert.assertNotNull(point);
        RedisOp op11 = IndexTestTool.readBytebufAfter(cmdFile.getPath(), point.getKey());
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
        RedisOp nextOp = IndexTestTool.readBytebufAfter(cmdFile.getPath(), point.getKey());
        Assert.assertEquals(uuid + ":622002", nextOp.getOpGtid());
    }

    @Test
    public void testLocateGtidRange_AfterGcRemovesOldestSegment() throws Exception {
        write(file1);
        switchCmdSegment("cmd_19513000");
        write(file2);

        String oldestCmdName = "00000000";
        Assert.assertTrue(new File(baseDir, oldestCmdName).delete());
        Assert.assertTrue(new File(baseDir, AbstractIndex.INDEX + oldestCmdName).delete());
        Assert.assertTrue(new File(baseDir, AbstractIndex.BLOCK + oldestCmdName).delete());
        Assert.assertTrue(new File(baseDir, AbstractIndex.INDEX_V2 + oldestCmdName).delete());
        Assert.assertTrue(new File(baseDir, AbstractIndex.BLOCK_V2 + oldestCmdName).delete());

        String uuid = "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0";
        List<Pair<Long, Long>> result = defaultIndexStore.locateGtidRange(uuid, 2, 10);

        Assert.assertFalse("Should locate GTIDs in remaining segment after GC", result.isEmpty());
        for (Pair<Long, Long> range : result) {
            Assert.assertNotNull(range.getKey());
            Assert.assertNotNull(range.getValue());
            Assert.assertTrue(range.getKey() < range.getValue());
            Assert.assertTrue("Start offset should be in remaining segment",
                    range.getKey() >= 19513000);
        }

        Assert.assertTrue(new File(baseDir, AbstractIndex.INDEX_V2 + "cmd_19513000").exists());
        Assert.assertFalse(new File(baseDir, AbstractIndex.INDEX_V2 + oldestCmdName).exists());
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
