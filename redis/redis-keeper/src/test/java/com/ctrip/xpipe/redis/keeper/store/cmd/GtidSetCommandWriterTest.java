package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.ctrip.xpipe.redis.core.store.CommandFile;
import com.ctrip.xpipe.redis.core.store.CommandFileOffsetGtidIndex;
import com.ctrip.xpipe.redis.core.store.CommandFileSegment;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import com.ctrip.xpipe.utils.FileUtils;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 * date 2022/5/23
 */
@RunWith(MockitoJUnitRunner.class)
public class GtidSetCommandWriterTest extends AbstractRedisOpParserTest {

    @Mock
    private CommandStore commandStore;

    @Mock
    private Logger delayTraceLogger;

    private File testDir;

    private File cmdFile;

    private CommandFileOffsetGtidIndex lastIndex;

    @Before
    public void setupGtidSetCommandWriterTest() throws IOException {
        testDir = new File(getTestFileDir());
        cmdFile = new File(testDir, "cmd_0");
        ControllableFile controllableFile = new DefaultControllableFile(cmdFile);
        for (int i = 1; i < 10; i++) {
            String rawCmd = mockCmdRaw("a1:" + i, "k" + i, "v" + i);
            controllableFile.getFileChannel().write(ByteBuffer.wrap(rawCmd.getBytes()));
        }
        controllableFile.close();

        lastIndex = new CommandFileOffsetGtidIndex(new GtidSet(""),
                new CommandFile(cmdFile, 0), 0);
        when(commandStore.findLastFileSegment()).thenReturn(new CommandFileSegment(lastIndex));
        when(commandStore.findLatestFile()).thenReturn(new CommandFile(cmdFile, 0));
        when(commandStore.findIndexFile(any())).thenReturn(new File(testDir, "idx_cmd_0"));
    }

    @Test
    public void testWriteCmdBytes() throws Exception {
        GtidSetCommandWriter writer = new GtidSetCommandWriter(new ArrayParser(), parser, commandStore, 1024 * 1024, delayTraceLogger);
        writer.initialize();

        writer.write(Unpooled.wrappedBuffer(mockCmdRaw("a1:10", "k10", "v10").getBytes()));
        writer.write(Unpooled.wrappedBuffer(mockCmdRaw("a1:11", "k11", "v11").getBytes()));

        Assert.assertEquals(new GtidSet("a1:1-11"), writer.getGtidSetContain());

        GtidCmdOneSegmentReader reader = new GtidCmdOneSegmentReader(commandStore, new CommandFileSegment(lastIndex), new ArrayParser(), parser);
        List<RedisOp> redisOps = new LinkedList<>();
        while(!reader.isFinish()) {
            RedisOp redisOp = reader.read();
            if (null == redisOp) continue;
            redisOps.add(redisOp);
        }

        Assert.assertEquals(11, redisOps.size());
        Assert.assertEquals("a1:1", redisOps.get(0).getOpGtid());
        Assert.assertEquals("a1:11", redisOps.get(10).getOpGtid());
    }

    @Test
    public void testWriteIndex() throws Exception {
        GtidSetCommandWriter writer = new GtidSetCommandWriter(new ArrayParser(), parser, commandStore, 1024 * 1024, delayTraceLogger);
        writer.initialize();
        writer.setBytesBetweenIndex(1);

        writer.write(Unpooled.wrappedBuffer(mockCmdRaw("a1:10", "k10", "v10").getBytes()));
        verify(commandStore).addIndex(any());

        File indexFile = new File(testDir, "idx_cmd_0");
        String rawStr = FileUtils.readFileAsString(indexFile.getAbsolutePath());
        CommandFileOffsetGtidIndex index = CommandFileOffsetGtidIndex.createFromRawString(rawStr, new CommandFile(cmdFile, 0));
        Assert.assertNotNull(index);
        Assert.assertEquals(cmdFile, index.getCommandFile().getFile());
        Assert.assertEquals(new GtidSet("a1:1-10"), index.getExcludedGtidSet());
        Assert.assertEquals(cmdFile.length(), index.getFileOffset());
    }

    private String mockCmdRaw(String gtid, String key, String val) {
        return "*5\r\n" +
                "$4\r\n" +
                "gtid\r\n" +
                "$" + gtid.length() + "\r\n"
                + gtid + "\r\n" +
                "$3\r\n" +
                "SET\r\n" +
                "$" + key.length() + "\r\n"
                + key + "\r\n" +
                "$" + val.length() + "\r\n" +
                val + "\r\n";
    }

}
