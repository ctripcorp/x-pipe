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

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultIndexStoreTest {

    private static final Logger log = LoggerFactory.getLogger(DefaultIndexStoreTest.class);
    String tempDir = System.getProperty("java.io.tmpdir");

    String baseDir = tempDir + "IndexStoreTest/";

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
    CommandStore commandStore;

    @Mock
    GtidCmdFilter gtidCmdFilter;

    @Before
    public void setUp() throws IOException {
        File dir = new File(baseDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        Path destinationPath = Paths.get(baseDir, "00000000");
        File tmpFile = new File(file1);
        if(!Files.exists(destinationPath)) {
            Files.copy(tmpFile.toPath(), destinationPath);
        }

        when(channel.size()).thenReturn(0l);

        when(commandFileContext.getChannel()).thenReturn(channel);

        when(commandFile.getFile()).thenReturn(new File("00000000"));
        when(commandFileContext.getCommandFile()).thenReturn(commandFile);

        when(writer.getFileContext()).thenReturn(commandFileContext);

        when(commandStore.getCommandWriter()).thenReturn(writer);

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandStore, commandStore, gtidCmdFilter);
        defaultIndexStore.initialize(writer);

    }

    @After
    public void tearDown() throws IOException {
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
        Assert.assertEquals(gtidSet.toString(), "a4f566ef50a85e1119f17f9b746728b48609a2ab:1-346526");

        long pre = System.currentTimeMillis();
        for(int i = 2; i < 346526; i++) {
            Pair<Long, GtidSet> point = defaultIndexStore.locateContinueGtidSet(new GtidSet("a4f566ef50a85e1119f17f9b746728b48609a2ab:1-" + i));
            Assert.assertEquals(point.getValue(), new GtidSet("a4f566ef50a85e1119f17f9b746728b48609a2ab:1-" + i));
            RedisOp redisOp =  IndexTestTool.readBytebufAfter(filePath, point.getKey());
            Assert.assertEquals(redisOp.getOpGtid(), "a4f566ef50a85e1119f17f9b746728b48609a2ab:" + (i + 1));
            if(i % 1000 == 0) {
                long now = System.currentTimeMillis();
                System.out.println("seek success " + i + "  " + (now - pre));
                pre = System.currentTimeMillis();
            }
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
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-800004");
        defaultIndexStore.switchCmdFile("cmd_19513000");
        write(file2);
        gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-800004,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-210654");
        long pre = System.currentTimeMillis();
        for(int i = 2; i <= 2000; i++) {
            Pair<Long, GtidSet> point = defaultIndexStore.locateContinueGtidSet(new GtidSet("a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-" + i));
            Assert.assertEquals(point.getValue().toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-800004,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-" + i);
            RedisOp redisOp = IndexTestTool.readBytebufAfter( file2, point.getKey() - 19513000);
            Assert.assertEquals(redisOp.getOpGtid(), "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:" + (i+1));
            if(i % 1000 == 0) {
                long now = System.currentTimeMillis();
                System.out.println("seek success " + i + "  " + (now - pre));
                pre = System.currentTimeMillis();
            }
        }
    }

    @Test
    public void testRecover() throws Exception {
        write(file1);
        // 不调用close
        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandStore, commandStore, gtidCmdFilter);
        defaultIndexStore.initialize(writer);
        for(int i = 800000; i < 800004; i++) {
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
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-800004");
    }

    @Test
    public void testRecover2() throws Exception {
        write(file1);

        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-800004");

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandStore, commandStore, gtidCmdFilter);
        defaultIndexStore.initialize(writer);


        gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-800004");

        defaultIndexStore.switchCmdFile("cmd_19513000");

        write(file2);

        gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-800004,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-210654");

        long pre = System.currentTimeMillis();
        for(int i = 1; i <= 2000; i++) {
            Pair<Long, GtidSet> point = defaultIndexStore.locateContinueGtidSet(new GtidSet("a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-" + i));
            RedisOp redisOp = IndexTestTool.readBytebufAfter(file2, point.getKey() - 19513000);
            Assert.assertEquals(redisOp.getOpGtid(), "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:" + (i+1));
            if(i % 1000 == 0) {
                long now = System.currentTimeMillis();
                System.out.println("seek success " + i + "  " + (now - pre));
                pre = System.currentTimeMillis();
            }
        }
    }

    @Test
    public void testBuildIndex() throws Exception {
        String cmdFile = "00000000";
        long pre = System.currentTimeMillis();
        defaultIndexStore.buildIndexFromCmdFile(cmdFile, 0);
        long now = System.currentTimeMillis();
        System.out.println("build index " + (now - pre));
        for(int i = 633744; i < 800004; i++) {
            if(i < 700002) {
                continue;
            }
            Pair<Long, GtidSet> point = defaultIndexStore.locateContinueGtidSet(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-" + i));
            RedisOp redisOp = IndexTestTool.readBytebufAfter(file1, point.getKey());
            Assert.assertEquals(redisOp.getOpGtid(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:" + (i + 1));
            if (i % 1000 == 0) {
                now = System.currentTimeMillis();
                System.out.println("build index success " + i + "  " + (now - pre));
                pre = System.currentTimeMillis();
            }
        }

        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-800004");

    }

    @Test
    public void testRecover3() throws IOException {
        write(file1);
        write(file2);
        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-800004,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-210654");

        DefaultControllableFile file = new DefaultControllableFile(baseDir + "00000000");

        File firstFile = new File(file1);
        file.setLength((int)firstFile.length());

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandStore, commandStore, gtidCmdFilter);
        defaultIndexStore.initialize(writer);

        gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-800004");

    }

    @Test
    public void testRecover4() throws IOException {
        write(file1);
        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-800004");

        DefaultControllableFile file = new DefaultControllableFile(baseDir + "index_00000000");

        file.setLength((int)file.size() - 10);

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandStore, commandStore, gtidCmdFilter);
        defaultIndexStore.initialize(writer);

        gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-800004");

    }

    @Test
    public void testRecover5() throws IOException {
        write(file1);
        GtidSet gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-800004");

        DefaultControllableFile file = new DefaultControllableFile(baseDir + "block_00000000");

        file.setLength((int)file.size() - 10);

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandStore, commandStore, gtidCmdFilter);
        defaultIndexStore.initialize(writer);

        gtidSet = defaultIndexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-800004");

    }

    @Test
    public void parseDirty() throws Exception {
        String path = "src/test/resources/GtidTest/dirty";

        write(path);

        String dirtyLength = "*7\r\n" +
                "$4\r\n" +
                "GTID\r\n" +
                "$47\r\n" +
                "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99";

        String dirtyLenth2 = "*7\r\n" +
                "$4\r\n" +
                "GTID\r\n" +
                "$47\r\n" +
                "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633746\r\n" +
                "$1\r\n" +
                "0\r\n" +
                "$4\r\n" +
                "HSET\r\n" +
                "$6\r\n" +
                "myhash\r\n" +
                "$20\r\n" +
                "element:__rand_int__\r\n" +
                "$";

        for(int i = 633747; i < 633758; i++) {
            Pair<Long, GtidSet> point = defaultIndexStore.locateContinueGtidSet(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-" + i));
            RedisOp redisOp = IndexTestTool.readBytebufAfter(path, point.getKey() + dirtyLength.getBytes().length + dirtyLenth2.getBytes().length);
            Assert.assertEquals(redisOp.getOpGtid(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:" + (i+1));
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
        defaultIndexStore = new DefaultIndexStore(baseDir, opParser, commandStore, commandStore, gtidCmdFilter);
        defaultIndexStore.initialize(writer);

        write(file1);

        Pair<Long, GtidSet> point =  defaultIndexStore.locateGtidSetWithFallbackToEnd(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-633744"));
        Pair<Long, GtidSet> point2 =  defaultIndexStore.locateGtidSetWithFallbackToEnd(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-633745"));
        System.out.println(point.getValue());
        System.out.println(point2.getValue());

        Assert.assertEquals(point.getKey(), point2.getKey());

        point = defaultIndexStore.locateContinueGtidSet(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-" + 633746));
        RedisOp redisOp = IndexTestTool.readBytebufAfter(file1, point.getKey() + 133);
        Assert.assertEquals(redisOp.getOpGtid(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633747");

    }
}
