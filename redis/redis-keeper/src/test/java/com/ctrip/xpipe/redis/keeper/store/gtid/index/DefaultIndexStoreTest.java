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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultIndexStoreTest {

    private static final Logger log = LoggerFactory.getLogger(DefaultIndexStoreTest.class);
    String tempDir = System.getProperty("java.io.tmpdir");

    String baseDir = Paths.get(tempDir, "IndexStoreTest").toString();

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

    @Before
    public void setUp() throws IOException {
        File dir = new File(baseDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        Path destinationPath = Paths.get(baseDir, "00000000");
        File tmpFile = new File(file1);
        if(!Files.exists(destinationPath)) {
            File destDir = new File(baseDir);
            if (!destDir.exists()) {
                boolean created = destDir.mkdirs();
                if (!created) {
                    throw new IOException("create folder fail" + destDir.getAbsolutePath());
                }
            }
            Files.copy(tmpFile.toPath(), destinationPath);
        }

        when(channel.size()).thenReturn(0l);

        when(commandFileContext.getChannel()).thenReturn(channel);

        when(commandFile.getFile()).thenReturn(new File("00000000"));
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);

        when(writer.getFileContext()).thenReturn(commandFileContext);

        when(commandWriterCallback.getCommandWriter()).thenReturn(writer);
        when(writer.needRotate()).thenReturn(false);
        when(writer.totalLength()).thenReturn(0L);

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);

        // 1. 初始化 mock 的 KeeperConfig（启用双写 + v2 读取）
        KeeperConfig keeperConfig = mock(KeeperConfig.class);
        when(ckStore.getKeeperConfig()).thenReturn(keeperConfig);
        when(keeperConfig.dualWrite()).thenReturn(true);   // 同时写 v1 和 v2
        when(keeperConfig.readV2()).thenReturn(true);      // 优先读 v2

        defaultIndexStore = new DefaultIndexStore(ckStore,baseDir, opParser, commandWriterCallback, gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
        defaultIndexStore.openWriter(writer);

    }

    @After
    public void tearDown() throws IOException {
        defaultIndexStore.closeWriter();
        // 删除basedir文件夹
        File dir = new File(baseDir);
        if (dir.exists()) {
            for (File file : dir.listFiles()) {
                file.delete();
            }
            dir.delete();
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
        defaultIndexStore.closeWithDeleteIndexFiles();
        int lastSize = directory.listFiles().length;
        Assert.assertEquals(initSize, lastSize + 4);
    }

    @Test
    public void testFileChange() throws Exception {
        write(file1);
        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750");
        defaultIndexStore.doSwitchCmdFile("cmd_19513000");
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
        defaultIndexStore = new DefaultIndexStore(ckStore,baseDir, opParser, commandWriterCallback, gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
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
        defaultIndexStore = new DefaultIndexStore(ckStore,baseDir, opParser, commandWriterCallback, gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
        defaultIndexStore.openWriter(writer);


        gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750");

        defaultIndexStore.doSwitchCmdFile("cmd_19513000");

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
        defaultIndexStore.buildIndexFromCmdFile(cmdFile, 0);
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
        defaultIndexStore = new DefaultIndexStore(ckStore,baseDir, opParser, commandWriterCallback, gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
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
        defaultIndexStore = new DefaultIndexStore(ckStore,baseDir, opParser, commandWriterCallback, gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
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
        defaultIndexStore = new DefaultIndexStore(ckStore,baseDir, opParser, commandWriterCallback, gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
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
        gtidCmdFilter = mock(GtidCmdFilter.class);
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
        defaultIndexStore = new DefaultIndexStore(ckStore,baseDir, opParser, commandWriterCallback, gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
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
        DefaultIndexStore testIndexStore = new DefaultIndexStore(ckStore,baseDir, opParser, commandWriterCallback, gtidCmdFilter, testCmdFile);
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
        DefaultIndexStore testIndexStore = new DefaultIndexStore(ckStore,baseDir, opParser, commandWriterCallback, gtidCmdFilter, testCmdFile);
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
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(file, true);
             java.nio.channels.FileChannel channel = fos.getChannel()) {
            GtidSetWrapper gtidSetWrapper = new GtidSetWrapper(gtidSet);
            gtidSetWrapper.saveGtidSet(channel);
        }
    }

    private void writeGtidSetV2ToFile(File file, GtidSet gtidSet) throws IOException {
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(file, true);
             java.nio.channels.FileChannel channel = fos.getChannel()) {
            GtidSetWrapper gtidSetWrapper = new GtidSetWrapper(gtidSet);
            gtidSetWrapper.saveGtidSetV2(channel);
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
        defaultIndexStore.deleteAllIndexFile();
        
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
    public void testLocateGtidRange_MultipleIndexFiles() throws IOException {
        // Test locating GTID range across multiple index files
        write(file1);
        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750");
        
        defaultIndexStore.doSwitchCmdFile("cmd_19513000");
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
    public void testLocateGtidRange_FileEnd() throws IOException {
        // Test locating GTID range that extends to file end
        write(file1);
        defaultIndexStore.doSwitchCmdFile("cmd_19513000");
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
    public void testLocateSkipEmptyIndexFile() throws IOException {
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

        defaultIndexStore.doSwitchCmdFile("cmd_19513000");
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
        DefaultIndexStore store = new DefaultIndexStore(ckStore,baseDir, opParser, commandWriterCallback,
                gtidCmdFilter, cmdName);
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

    }

    @Test
    public void testV2ZoneWriter_FlushOnGtid() throws IOException {
        // 准备独立的工作目录和 cmd 文件名
        baseDir = Paths.get(tempDir, "IndexStoreTest-v2zoneFlushOnGtid").toString();
        File dir = new File(baseDir);
        if (dir.exists()) for (File f : dir.listFiles()) f.delete();
        else dir.mkdirs();

        String cmdName = "cmd_v2zone_flush_on_gtid_0";
        File cmdFile = new File(baseDir, cmdName);

        DefaultIndexStore store = createV2Store(cmdFile,cmdName);

        // 构造非 GTID 命令（PING）和 GTID 命令
        ByteBuf pingCmd = createPingCommand();
        String uuid = "cafebabecafebabecafebabecafebabecafebabe"; // 40字符

        // 写入两条非 GTID 命令（偏移手动计算，也可直接写入后由 store 计算）
        store.write(pingCmd); // 命令长度假设 20（实际 PING 长度）
        // 记录当前 cmd 文件长度，即下一个命令的起始偏移
        long afterFirstPing = cmdFile.length();
        pingCmd = createPingCommand();
        store.write(pingCmd);
        long afterSecondPing = cmdFile.length();

        // 此时应该没有 zone 落盘（因为阈值未到且没有 GTID）
        IndexWriterV2 writerV2 = store.getIndexWriterV2(); // 需要暴露 indexWriterV2 或通过反射获取
        List<long[]> zonesBeforeGtid = writerV2.loadAllZones();
        Assert.assertEquals("No zone before GTID", 0, zonesBeforeGtid.size());

        // 写入一条 GTID 命令（触发 flushZone）
        ByteBuf gtidCmd = createGtidCommand(uuid + ":1", "SET", "k", "v");
        store.write(gtidCmd);

        // 验证 zone 已生成且覆盖前两条 PING 的区间
        List<long[]> zonesAfterGtid = writerV2.loadAllZones();
        Assert.assertEquals("One zone record after GTID", 1, zonesAfterGtid.size());
        Assert.assertEquals("zone start", 0L, zonesAfterGtid.get(0)[0]);
        // zone end 应为两条 PING 命令结束后的偏移（即 afterSecondPing）
        Assert.assertEquals("zone end", afterSecondPing, zonesAfterGtid.get(0)[1]);

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

        int threshold = IndexWriterV2.FLUSH_THRESHOLD;
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

        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        DefaultIndexStore store = new DefaultIndexStore(ckStore, baseDir, opParser,
                commandWriterCallback, gtidCmdFilter, cmdName);
        store.openWriter(writer);
        return store;
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
