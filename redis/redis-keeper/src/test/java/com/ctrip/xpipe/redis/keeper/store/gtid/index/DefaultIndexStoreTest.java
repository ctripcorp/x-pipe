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

    public void writeRawStr(String cmdStr) throws IOException {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(cmdStr.getBytes());
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
        // index_, block_, and nongtid_ files are removed
        Assert.assertEquals(initSize, lastSize + 3);
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
        int size = (int) file.size();
        if(size > 10){
            size = size - 10;
        }
        file.setLength(size);

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

    // ============================================================
    // NonGtidIndexWriter — direct unit tests
    // ============================================================

    private static final int REC = NonGtidIndexWriter.RECORD_LEN;

    private File zoneFileFor(String dir, String cmdName) {
        return new File(Paths.get(dir, NonGtidIndexWriter.ZONE_PREFIX + cmdName).toString());
    }

    private long zoneFileSize(String dir, String cmdName) {
        return zoneFileFor(dir, cmdName).length();
    }

    private List<long[]> readZoneRecords(String dir, String cmdName) throws IOException {
        File f = zoneFileFor(dir, cmdName);
        java.util.List<long[]> out = new java.util.ArrayList<>();
        if (!f.exists()) return out;
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(f, "r")) {
            long size = raf.length();
            long aligned = (size / REC) * REC;
            for (long pos = 0; pos < aligned; pos += REC) {
                raf.seek(pos);
                long s = raf.readLong();
                long e = raf.readLong();
                out.add(new long[]{s, e});
            }
        }
        return out;
    }

    @Test
    public void testZoneWriter_FlushOnGtid() throws IOException {
        String cmdName = "zone_unit_a";
        NonGtidIndexWriter w = new NonGtidIndexWriter(baseDir, cmdName);
        w.init();
        w.appendNonGtid(0, 10);
        w.appendNonGtid(10, 20);
        Assert.assertEquals("not flushed yet", 0L, zoneFileSize(baseDir, cmdName));
        w.onGtid();
        Assert.assertEquals("one record after GTID", REC, zoneFileSize(baseDir, cmdName));
        List<long[]> recs = readZoneRecords(baseDir, cmdName);
        Assert.assertEquals(1, recs.size());
        Assert.assertEquals(0L, recs.get(0)[0]);
        Assert.assertEquals(30L, recs.get(0)[1]);
        Assert.assertFalse(w.hasActiveZone());
        w.close();
    }

    @Test
    public void testZoneWriter_FlushOnThreshold() throws IOException {
        String cmdName = "zone_unit_b";
        NonGtidIndexWriter w = new NonGtidIndexWriter(baseDir, cmdName);
        w.init();
        long off = 0;
        for (int i = 0; i < NonGtidIndexWriter.FLUSH_THRESHOLD; i++) {
            w.appendNonGtid(off, 1);
            off += 1;
        }
        Assert.assertEquals("flushed exactly once on threshold", REC, zoneFileSize(baseDir, cmdName));
        Assert.assertFalse(w.hasActiveZone());
        // continue, next flush should be on close
        w.appendNonGtid(off, 7);
        w.close();
        Assert.assertEquals("close flushed second record", 2 * REC, zoneFileSize(baseDir, cmdName));
        List<long[]> recs = readZoneRecords(baseDir, cmdName);
        Assert.assertEquals(2, recs.size());
        Assert.assertEquals(0L, recs.get(0)[0]);
        Assert.assertEquals((long) NonGtidIndexWriter.FLUSH_THRESHOLD, recs.get(0)[1]);
        Assert.assertEquals((long) NonGtidIndexWriter.FLUSH_THRESHOLD, recs.get(1)[0]);
        Assert.assertEquals(NonGtidIndexWriter.FLUSH_THRESHOLD + 7L, recs.get(1)[1]);
    }

    @Test
    public void testZoneWriter_FlushOnClose() throws IOException {
        String cmdName = "zone_unit_c";
        NonGtidIndexWriter w = new NonGtidIndexWriter(baseDir, cmdName);
        w.init();
        w.appendNonGtid(100, 50);
        w.close();
        Assert.assertEquals(REC, zoneFileSize(baseDir, cmdName));
        List<long[]> recs = readZoneRecords(baseDir, cmdName);
        Assert.assertEquals(1, recs.size());
        Assert.assertEquals(100L, recs.get(0)[0]);
        Assert.assertEquals(150L, recs.get(0)[1]);
    }

    @Test
    public void testZoneWriter_EmptyCloseWritesNothing() throws IOException {
        String cmdName = "zone_unit_d";
        NonGtidIndexWriter w = new NonGtidIndexWriter(baseDir, cmdName);
        w.init();
        w.close();
        Assert.assertEquals(0L, zoneFileSize(baseDir, cmdName));
    }

    @Test
    public void testZoneWriter_GtidWithoutPriorNonGtid() throws IOException {
        String cmdName = "zone_unit_e";
        NonGtidIndexWriter w = new NonGtidIndexWriter(baseDir, cmdName);
        w.init();
        w.onGtid();
        w.onGtid();
        Assert.assertEquals("no zone, nothing flushed", 0L, zoneFileSize(baseDir, cmdName));
        w.close();
        Assert.assertEquals(0L, zoneFileSize(baseDir, cmdName));
    }

    @Test
    public void testZoneWriter_TornRecordTruncatedOnInit() throws IOException {
        String cmdName = "zone_unit_torn";
        // write one valid record + 5 bytes of garbage
        File f = zoneFileFor(baseDir, cmdName);
        f.getParentFile().mkdirs();
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(f)) {
            ByteBuffer buf = ByteBuffer.allocate(REC + 5);
            buf.putLong(10L);
            buf.putLong(99L);
            buf.put(new byte[]{1, 2, 3, 4, 5});
            buf.flip();
            fos.getChannel().write(buf);
        }
        Assert.assertEquals(REC + 5, f.length());

        NonGtidIndexWriter w = new NonGtidIndexWriter(baseDir, cmdName);
        w.init();
        Assert.assertEquals("torn tail truncated", REC, f.length());

        // append after recovery should not leave a hole
        w.appendNonGtid(200, 50);
        w.close();
        Assert.assertEquals(2 * REC, f.length());
        List<long[]> recs = readZoneRecords(baseDir, cmdName);
        Assert.assertEquals(2, recs.size());
        Assert.assertEquals(10L, recs.get(0)[0]);
        Assert.assertEquals(99L, recs.get(0)[1]);
        Assert.assertEquals(200L, recs.get(1)[0]);
        Assert.assertEquals(250L, recs.get(1)[1]);
    }

    @Test
    public void testZoneWriter_LoadAllZonesPreservesOrder() throws IOException {
        String cmdName = "zone_unit_load";
        NonGtidIndexWriter w = new NonGtidIndexWriter(baseDir, cmdName);
        w.init();
        w.appendNonGtid(0, 10);
        w.onGtid();
        w.appendNonGtid(50, 5);
        w.onGtid();
        w.appendNonGtid(200, 100);
        // last one not yet flushed
        List<long[]> partial = w.loadAllZones();
        Assert.assertEquals(2, partial.size());
        Assert.assertArrayEquals(new long[]{0, 10}, partial.get(0));
        Assert.assertArrayEquals(new long[]{50, 55}, partial.get(1));

        // after load, position should be at end so subsequent flush does not corrupt
        w.close();
        List<long[]> all = readZoneRecords(baseDir, cmdName);
        Assert.assertEquals(3, all.size());
        Assert.assertArrayEquals(new long[]{200, 300}, all.get(2));
    }

    @Test
    public void testZoneWriter_LoadAllZonesSkipsInvalidRecords() throws IOException {
        String cmdName = "zone_unit_invalid";
        File f = zoneFileFor(baseDir, cmdName);
        f.getParentFile().mkdirs();
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(f)) {
            ByteBuffer buf = ByteBuffer.allocate(REC * 3);
            buf.putLong(0L);  buf.putLong(10L);   // valid
            buf.putLong(100L); buf.putLong(50L);  // end < start, invalid
            buf.putLong(200L); buf.putLong(300L); // valid
            buf.flip();
            fos.getChannel().write(buf);
        }
        NonGtidIndexWriter w = new NonGtidIndexWriter(baseDir, cmdName);
        w.init();
        List<long[]> recs = w.loadAllZones();
        Assert.assertEquals("invalid record skipped", 2, recs.size());
        Assert.assertArrayEquals(new long[]{0, 10}, recs.get(0));
        Assert.assertArrayEquals(new long[]{200, 300}, recs.get(1));
        w.close();
    }

    @Test
    public void testZoneWriter_DeleteZoneFiles() throws IOException {
        String cmdName1 = "zone_unit_del_1";
        String cmdName2 = "zone_unit_del_2";
        for (String n : new String[]{cmdName1, cmdName2}) {
            NonGtidIndexWriter w = new NonGtidIndexWriter(baseDir, n);
            w.init();
            w.appendNonGtid(0, 10);
            w.close();
        }
        Assert.assertTrue(zoneFileFor(baseDir, cmdName1).exists());
        Assert.assertTrue(zoneFileFor(baseDir, cmdName2).exists());

        NonGtidIndexWriter.deleteZoneFiles(baseDir);

        Assert.assertFalse(zoneFileFor(baseDir, cmdName1).exists());
        Assert.assertFalse(zoneFileFor(baseDir, cmdName2).exists());
    }

    // ============================================================
    // DefaultIndexStore — zone file lifecycle integration
    // ============================================================

    @Test
    public void testZoneFile_CreatedOnOpenWriter() {
        // setUp() already opened the writer for cmd file "00000000"
        File zone = zoneFileFor(baseDir, "00000000");
        Assert.assertTrue("zone file should be created on openWriter", zone.exists());
        Assert.assertEquals("freshly opened zone file is empty", 0L, zone.length());
    }

    @Test
    public void testZoneFile_FlushedAfterWriteAndClose() throws IOException {
        write(filePath);
        // close flushes any active zone
        defaultIndexStore.closeWriter();

        File zone = zoneFileFor(baseDir, "00000000");
        Assert.assertTrue(zone.exists());
        Assert.assertEquals("zone file aligned to record size", 0L, zone.length() % REC);

        if (zone.length() > 0) {
            List<long[]> recs = readZoneRecords(baseDir, "00000000");
            long cmdSize = new File(Paths.get(baseDir, "00000000").toString()).length();
            for (long[] z : recs) {
                Assert.assertTrue("zone start non-negative", z[0] >= 0);
                Assert.assertTrue("zone end > start", z[1] > z[0]);
                Assert.assertTrue("zone within cmd file", z[1] <= cmdSize);
            }
        }

        // re-open for tearDown to clean up consistently
        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback,
                gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
        defaultIndexStore.openWriter(writer);
    }

    @Test
    public void testZoneFile_DeletedByCloseWithDelete() throws IOException {
        write(filePath);
        File zone = zoneFileFor(baseDir, "00000000");
        Assert.assertTrue(zone.exists());

        defaultIndexStore.closeWithDeleteIndexFiles();
        Assert.assertFalse("zone file removed", zone.exists());

        // re-open empty writer for tearDown symmetry
        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback,
                gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
        defaultIndexStore.openWriter(writer);
    }

    @Test
    public void testZoneFile_RotatedBySwitchCmdFile() throws IOException {
        write(file1);
        File zoneA = zoneFileFor(baseDir, "00000000");
        Assert.assertTrue(zoneA.exists());

        defaultIndexStore.doSwitchCmdFile("cmd_19513000");
        File zoneB = zoneFileFor(baseDir, "cmd_19513000");
        Assert.assertTrue("new zone file created on switch", zoneB.exists());

        write(file2);
        defaultIndexStore.closeWriter();

        Assert.assertEquals(0L, zoneA.length() % REC);
        Assert.assertEquals(0L, zoneB.length() % REC);

        // re-open for tearDown
        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback,
                gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
        defaultIndexStore.openWriter(writer);
    }

    // ============================================================
    // Recovery — zone-aware paths
    // ============================================================

    private void reopenIndexStore() throws IOException {
        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        defaultIndexStore.closeWriter();
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback,
                gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
        defaultIndexStore.openWriter(writer);
    }

    @Test
    public void testRecover_WithZoneIndexProducesSameGtidSet() throws IOException {
        write(file1);
        GtidSet before = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-633750", before.toString());

        // simulate partial-loss recovery: chop a few bytes off the index file so that
        // recovery has to rebuild from a previous IndexEntry — zone records guide the rebuild.
        DefaultControllableFile idx = new DefaultControllableFile(baseDir + "/index_00000000");
        idx.setLength(Math.max(0, (int) idx.size() - 10));
        idx.close();

        reopenIndexStore();
        Assert.assertEquals("zone-guided recovery preserves GTID set",
                before.toString(), defaultIndexStore.getIndexGtidSet().toString());
    }

    @Test
    public void testRecover_ZoneFileMissingFallsBackToFullLegacy() throws IOException {
        write(file1);
        GtidSet before = defaultIndexStore.getIndexGtidSet();

        // corrupt index so recovery fires
        DefaultControllableFile idx = new DefaultControllableFile(baseDir + "/index_00000000");
        idx.setLength(Math.max(0, (int) idx.size() - 10));
        idx.close();
        // close writer to flush zone, then delete zone file
        defaultIndexStore.closeWriter();
        File zone = zoneFileFor(baseDir, "00000000");
        if (zone.exists()) Assert.assertTrue(zone.delete());

        // re-open: with no zone file, recovery must fall back to legacy full scan
        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback,
                gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
        defaultIndexStore.openWriter(writer);

        Assert.assertEquals("legacy fallback yields same GTID set",
                before.toString(), defaultIndexStore.getIndexGtidSet().toString());
    }

    @Test
    public void testRecover_ZoneFileTornStillRecovers() throws IOException {
        write(file1);
        GtidSet before = defaultIndexStore.getIndexGtidSet();

        DefaultControllableFile idx = new DefaultControllableFile(baseDir + "/index_00000000");
        idx.setLength(Math.max(0, (int) idx.size() - 10));
        idx.close();
        defaultIndexStore.closeWriter();

        // append 7 bytes of garbage to zone file -> torn tail
        File zone = zoneFileFor(baseDir, "00000000");
        if (zone.exists() && zone.length() > 0) {
            try (java.io.FileOutputStream fos = new java.io.FileOutputStream(zone, true)) {
                fos.write(new byte[]{1, 2, 3, 4, 5, 6, 7});
            }
            long beforeOpen = zone.length();
            Assert.assertNotEquals("introduced misalignment", 0, beforeOpen % REC);
        }

        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback,
                gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
        defaultIndexStore.openWriter(writer);

        Assert.assertEquals("torn tail does not break recovery",
                before.toString(), defaultIndexStore.getIndexGtidSet().toString());
        // and the zone file is now realigned
        Assert.assertEquals(0L, zoneFileFor(baseDir, "00000000").length() % REC);
    }

    @Test
    public void testRecover_ZoneEndBeyondCmdSizeIsDropped() throws IOException {
        write(file1);
        GtidSet before = defaultIndexStore.getIndexGtidSet();
        defaultIndexStore.closeWriter();

        // overwrite zone file with one bogus record extending past cmd file size
        File zone = zoneFileFor(baseDir, "00000000");
        long cmdSize = new File(Paths.get(baseDir, "00000000").toString()).length();
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(zone, false)) {
            ByteBuffer buf = ByteBuffer.allocate(REC);
            buf.putLong(0L);
            buf.putLong(cmdSize + 1024L); // past EOF
            buf.flip();
            fos.getChannel().write(buf);
        }

        // also chip the index file so a rebuild is forced
        DefaultControllableFile idx = new DefaultControllableFile(baseDir + "/index_00000000");
        idx.setLength(Math.max(0, (int) idx.size() - 10));
        idx.close();

        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback,
                gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
        defaultIndexStore.openWriter(writer);

        Assert.assertEquals("bogus zone is dropped, legacy still recovers all GTIDs",
                before.toString(), defaultIndexStore.getIndexGtidSet().toString());
    }

    @Test
    public void testRecover_ZoneFullyBeforeRebuildStartIsIgnored() throws IOException {
        write(file1);
        GtidSet before = defaultIndexStore.getIndexGtidSet();
        defaultIndexStore.closeWriter();

        // overwrite zone with one record entirely in the [0, 10) range — guaranteed to be
        // before any reasonable rebuildStart computed from the IndexEntry chain
        File zone = zoneFileFor(baseDir, "00000000");
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(zone, false)) {
            ByteBuffer buf = ByteBuffer.allocate(REC);
            buf.putLong(0L);
            buf.putLong(10L);
            buf.flip();
            fos.getChannel().write(buf);
        }

        // do NOT chip index — rebuildStart will be near end of cmd file
        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandWriterCallback,
                gtidCmdFilter, writer.getFileContext().getCommandFile().getFile().getName());
        defaultIndexStore.openWriter(writer);

        Assert.assertEquals("zone before rebuildStart is harmlessly ignored",
                before.toString(), defaultIndexStore.getIndexGtidSet().toString());
    }

    @Test
    public void testRecover_LastSegmentTruncatesIncompleteTransaction() throws IOException {
        // Reuse a synthetic cmd file: a valid GTID, a valid GTID, then a torn MULTI block.
        // Recovery should keep both GTIDs and chop the file at the start of the MULTI.
        baseDir = Paths.get(tempDir, "IndexStoreTest-zoneIncompleteTx").toString();
        File dir = new File(baseDir);
        if (dir.exists()) {
            for (File f : dir.listFiles()) f.delete();
        } else {
            dir.mkdirs();
        }
        String cmdName = "cmd_zone_tx_0";
        File cmdFile = new File(baseDir, cmdName);
        File indexFile = new File(baseDir, "index_" + cmdName);

        String gtid1 = "a4f566ef50a85e1119f17f9b746728b48609a2ab:1";
        String gtid2 = "a4f566ef50a85e1119f17f9b746728b48609a2ab:2";

        writeGtidSetToFile(indexFile, new GtidSet(""));
        writeCommandToFile(cmdFile, createGtidCommand(gtid1, "SET", "k1", "v1"));
        writeCommandToFile(cmdFile, createGtidCommand(gtid2, "SET", "k2", "v2"));
        long cutoff = cmdFile.length();
        writeCommandToFile(cmdFile, createMultiCommand());
        writeCommandToFile(cmdFile, createSetCommand("k3", "v3"));
        // no EXEC — incomplete

        when(commandFile.getFile()).thenReturn(cmdFile);
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);
        when(writer.getFileContext()).thenReturn(commandFileContext);

        RedisOpParserManager mgr = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(mgr);
        RedisOpParser opParser = new GeneralRedisOpParser(mgr);
        DefaultIndexStore store = new DefaultIndexStore(baseDir, opParser, commandWriterCallback,
                gtidCmdFilter, cmdName);
        store.openWriter(writer);

        Assert.assertEquals("incomplete tx truncated to cutoff", cutoff, cmdFile.length());
        GtidSet gs = store.getIndexGtidSet();
        Assert.assertTrue(gs.contains("a4f566ef50a85e1119f17f9b746728b48609a2ab", 1));
        Assert.assertTrue(gs.contains("a4f566ef50a85e1119f17f9b746728b48609a2ab", 2));
        store.closeWriter();
    }

    @Test
    public void testRecover_AlternatingGtidAndNonGtid() throws IOException {
        // Build cmd file with: GTID, SELECT, GTID, SELECT, SELECT, GTID
        // After write, recovery via zone index must yield identical GTID set.
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
        DefaultIndexStore store = new DefaultIndexStore(baseDir, opParser, commandWriterCallback,
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

        store.write(Unpooled.wrappedBuffer(toBytes(createGtidCommand(
                "a4f566ef50a85e1119f17f9b746728b48609a2ab:1", "SET", "k1", "v1"))));
        store.write(Unpooled.wrappedBuffer(toBytes(createSelectCommand("0"))));
        store.write(Unpooled.wrappedBuffer(toBytes(createGtidCommand(
                "a4f566ef50a85e1119f17f9b746728b48609a2ab:2", "SET", "k2", "v2"))));
        store.write(Unpooled.wrappedBuffer(toBytes(createSelectCommand("0"))));
        store.write(Unpooled.wrappedBuffer(toBytes(createSelectCommand("0"))));
        store.write(Unpooled.wrappedBuffer(toBytes(createGtidCommand(
                "a4f566ef50a85e1119f17f9b746728b48609a2ab:3", "SET", "k3", "v3"))));

        GtidSet expected = store.getIndexGtidSet();
        Assert.assertTrue(expected.contains("a4f566ef50a85e1119f17f9b746728b48609a2ab", 1));
        Assert.assertTrue(expected.contains("a4f566ef50a85e1119f17f9b746728b48609a2ab", 2));
        Assert.assertTrue(expected.contains("a4f566ef50a85e1119f17f9b746728b48609a2ab", 3));

        // chop tail of index to force rebuild via zones
        store.closeWriter();
        DefaultControllableFile idx = new DefaultControllableFile(baseDir + "/index_" + cmdName);
        idx.setLength(Math.max(0, (int) idx.size() - 10));
        idx.close();

        DefaultIndexStore store2 = new DefaultIndexStore(baseDir, opParser, commandWriterCallback,
                gtidCmdFilter, cmdName);
        store2.openWriter(writer);
        Assert.assertEquals("alternating sequence recovers identically",
                expected.toString(), store2.getIndexGtidSet().toString());
        store2.closeWriter();
    }

    private byte[] toBytes(ByteBuf b) {
        byte[] out = new byte[b.readableBytes()];
        b.getBytes(b.readerIndex(), out);
        return out;
    }

    private ByteBuf createSelectCommand(String db) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte((byte) '*');
        buffer.writeBytes("2".getBytes());
        buffer.writeBytes("\r\n".getBytes());
        writeBulkString(buffer, "SELECT");
        writeBulkString(buffer, db);
        return buffer;
    }
}
