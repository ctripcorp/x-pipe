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

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    CommandWriter commandWriterForCallback;

    @Mock
    GtidCmdFilter gtidCmdFilter;

    @Mock
    IndexWriter indexWriter;

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

        when(commandWriterCallback.getCommandWriter()).thenReturn(commandWriterForCallback);
        when(commandWriterForCallback.rotateFileIfNecessary()).thenReturn(false);
        when(commandWriterForCallback.totalLength()).thenReturn(0L);

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback, gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
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
        Assert.assertEquals(initSize, lastSize + 2);
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
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback, gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
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
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback, gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
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
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback, gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
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
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback, gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
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

        file.setLength((int)file.size() - 10);

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore.closeWriter();
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback, gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
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
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback, gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
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
        File cmdFile = new File(baseDir, testCmdFile);
        File indexFile = new File(baseDir, testIndexFile);

        // First, write some valid commands with GTID
        String gtid1 = "a4f566ef50a85e1119f17f9b746728b48609a2ab:1";
        String gtid2 = "a4f566ef50a85e1119f17f9b746728b48609a2ab:2";

        // Write first complete GTID command
        writeCommandToFile(cmdFile, createGtidCommand(gtid1, "SET", "key1", "value1"));
        writeGtidSetToFile(indexFile, new GtidSet(""));
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
        DefaultIndexStore testIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback, gtidCmdFilter, testCmdFile);
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
        File cmdFile = new File(baseDir, testCmdFile);
        File indexFile = new File(baseDir, testIndexFile);

        // Write multiple valid GTID commands
        String gtid1 = "a4f566ef50a85e1119f17f9b746728b48609a2ab:1";
        String gtid2 = "a4f566ef50a85e1119f17f9b746728b48609a2ab:2";
        String gtid3 = "a4f566ef50a85e1119f17f9b746728b48609a2ab:3";

        writeGtidSetToFile(indexFile, new GtidSet(""));
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
        DefaultIndexStore testIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback, gtidCmdFilter, testCmdFile);
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
}
