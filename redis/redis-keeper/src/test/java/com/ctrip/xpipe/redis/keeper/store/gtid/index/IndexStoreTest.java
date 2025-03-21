package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class IndexStoreTest {

    String tempDir = System.getProperty("java.io.tmpdir");

    String baseDir = tempDir + "IndexStoreTest/";

    String filePath = "src/test/resources/GtidTest/appendonly.aof";

    String file1 = "src/test/resources/GtidTest/00000000.aof";
    String file2 = "src/test/resources/GtidTest/19513000.aof";


    String cmdDir = "src/test/resources/GtidTest/";

    private IndexStore indexStore;

    @Before
    public void setUp() throws IOException {
        File dir = new File(baseDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        Path destinationPath = Paths.get(baseDir, "00000000");
        File tmpFile = new File(file1);
        Files.copy(tmpFile.toPath(), destinationPath);

        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        indexStore = new IndexStore(baseDir, "00000000", 0, opParser, new GtidSet(""));
        indexStore.init();


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
            indexStore.write(byteBuf);
        }
    }

    @Test
    public void testSearch() throws Exception {
        write(filePath);
        long pre = System.currentTimeMillis();
        for(int i = 2; i < 346526; i++) {
            ContinuePoint point = indexStore.locateContinueGtidSet(new GtidSet("a4f566ef50a85e1119f17f9b746728b48609a2ab:1-" + i));
            ByteBuf byteBuf = IndexTestTool.readBytebufAfter(filePath, point.getOffset());
            RedisOp redisOp = IndexTestTool.readByteBuf(byteBuf);
            Assert.assertEquals(redisOp.getOpGtid(), "a4f566ef50a85e1119f17f9b746728b48609a2ab:" + i);
            if(i % 1000 == 0) {
                long now = System.currentTimeMillis();
                System.out.println("seek success " + i + "  " + (now - pre));
                pre = System.currentTimeMillis();
            }
        }
    }

    @Test
    public void testFileChange() throws Exception {
        write(file1);
        GtidSet gtidSet = indexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-800004");
        indexStore.switchCmdFile("19513000");
        write(file2);
        gtidSet = indexStore.getIndexGtidSet();
        Assert.assertEquals(gtidSet.toString(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:633744-800004,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-210654");
        long pre = System.currentTimeMillis();
        for(int i = 1; i <= 2000; i++) {
            ContinuePoint point = indexStore.locateContinueGtidSet(new GtidSet("a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-" + i));
            ByteBuf byteBuf = IndexTestTool.readBytebufAfter( cmdDir + point.getFileName() + ".aof", point.getOffset());
            RedisOp redisOp = IndexTestTool.readByteBuf(byteBuf);
            Assert.assertEquals(redisOp.getOpGtid(), "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:" + i);
            if(i % 1000 == 0) {
                long now = System.currentTimeMillis();
                System.out.println("seek success " + i + "  " + (now - pre));
                pre = System.currentTimeMillis();
            }
        }
        pre = System.currentTimeMillis();
        for(int i = 633744; i <= 800004; i++) {
            ContinuePoint point = indexStore.locateContinueGtidSet(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-" + i));
            ByteBuf byteBuf = IndexTestTool.readBytebufAfter(cmdDir + point.getFileName() + ".aof", point.getOffset());
            RedisOp redisOp = IndexTestTool.readByteBuf(byteBuf);
            Assert.assertEquals(redisOp.getOpGtid(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:" + i);
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
        indexStore = new IndexStore(baseDir, "00000000", 0, opParser, new GtidSet(""));
        indexStore.init();
        for(int i = 800000; i < 800004; i++) {
            ContinuePoint point = indexStore.locateContinueGtidSet(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-" + i));
            ByteBuf byteBuf = IndexTestTool.readBytebufAfter(file1, point.getOffset());
            RedisOp redisOp = IndexTestTool.readByteBuf(byteBuf);
            Assert.assertEquals(redisOp.getOpGtid(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:" + i);
        }
    }

    @Test
    public void testRecover2() throws Exception {
        write(file1);
        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser opParser = new GeneralRedisOpParser(redisOpParserManager);
        indexStore = new IndexStore(baseDir, "00000000", 0, opParser, new GtidSet(""));
        indexStore.init();
        write(file2);
        for(int i = 800000; i < 800004; i++) {
            ContinuePoint point = indexStore.locateContinueGtidSet(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-" + i));
            ByteBuf byteBuf = IndexTestTool.readBytebufAfter(file1, point.getOffset());
            RedisOp redisOp = IndexTestTool.readByteBuf(byteBuf);
            Assert.assertEquals(redisOp.getOpGtid(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:" + i);
        }

        long pre = System.currentTimeMillis();
        for(int i = 1; i <= 2000; i++) {
            ContinuePoint point = indexStore.locateContinueGtidSet(new GtidSet("a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:1-" + i));
            ByteBuf byteBuf = IndexTestTool.readBytebufAfter(file2, point.getOffset());
            RedisOp redisOp = IndexTestTool.readByteBuf(byteBuf);
            Assert.assertEquals(redisOp.getOpGtid(), "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:" + i);
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
        indexStore.buildIndexFromCmdFile(cmdFile, 0);
        long pre = System.currentTimeMillis();
        for(int i = 633744; i < 800004; i++) {
            ContinuePoint point = indexStore.locateContinueGtidSet(new GtidSet("f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:1-" + i));
            ByteBuf byteBuf = IndexTestTool.readBytebufAfter(file1, point.getOffset());
            RedisOp redisOp = IndexTestTool.readByteBuf(byteBuf);
            Assert.assertEquals(redisOp.getOpGtid(), "f9c9211ae82b9c4a4ea40eecd91d5d180c9c99f0:" + i);
            if (i % 1000 == 0) {
                long now = System.currentTimeMillis();
                System.out.println("build index success " + i + "  " + (now - pre));
                pre = System.currentTimeMillis();
            }
        }
    }

}
