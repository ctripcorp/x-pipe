package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.ctrip.xpipe.redis.core.store.CommandFile;
import com.ctrip.xpipe.redis.core.store.CommandFileOffsetGtidIndex;
import com.ctrip.xpipe.redis.core.store.CommandFileSegment;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


/**
 * @author lishanglin
 * date 2022/5/22
 */
@RunWith(MockitoJUnitRunner.class)
public class GtidCmdOneSegmentReaderTest extends AbstractRedisOpParserTest {

    @Mock
    private CommandStore commandStore;

    @Test
    public void testReadRightBoundClosedSegment() throws Exception {
        CommandFileOffsetGtidIndex startIndex = new CommandFileOffsetGtidIndex(new GtidSet("a1:1-10,b1:1-5"),
                new CommandFile(new File("./src/test/resources/GtidSetCommandReaderTest/cmd_1"), 0),
                0);
        CommandFileOffsetGtidIndex endIndex = new CommandFileOffsetGtidIndex(new GtidSet("a1:1-15,b1:1-5"),
                new CommandFile(new File("./src/test/resources/GtidSetCommandReaderTest/cmd_1"), 0),
                260);

        GtidCmdOneSegmentReader reader = new GtidCmdOneSegmentReader(commandStore,
                new CommandFileSegment(startIndex, endIndex), new ArrayParser(), parser);

        List<RedisOp> redisOps = new ArrayList<>();
        while(!reader.isFinish()) {
            RedisOp redisOp = reader.read();
            if (null != redisOp) redisOps.add(redisOp);
        }

        Assert.assertEquals(5, redisOps.size());
        Assert.assertEquals("a1:11", redisOps.get(0).getOpGtid());
        Assert.assertEquals("a1:15", redisOps.get(4).getOpGtid());
    }

    @Test
    public void testReadCrossFileSegment() throws Exception {
        CommandFileOffsetGtidIndex startIndex = new CommandFileOffsetGtidIndex(new GtidSet("a1:1-2,b1:1-5"),
                new CommandFile(new File("./src/test/resources/GtidSetCommandReaderTest/cmd_0"), 0),
                98);
        CommandFileOffsetGtidIndex endIndex = new CommandFileOffsetGtidIndex(new GtidSet("a1:1-15,b1:1-5"),
                new CommandFile(new File("./src/test/resources/GtidSetCommandReaderTest/cmd_1"), 0),
                260);
        Mockito.when(commandStore.findNextFile(startIndex.getCommandFile().getFile())).thenReturn(endIndex.getCommandFile());

        GtidCmdOneSegmentReader reader = new GtidCmdOneSegmentReader(commandStore,
                new CommandFileSegment(startIndex, endIndex), new ArrayParser(), parser);

        List<RedisOp> redisOps = new ArrayList<>();
        while(!reader.isFinish()) {
            RedisOp redisOp = reader.read();
            if (null != redisOp) redisOps.add(redisOp);
        }

        Assert.assertEquals(13, redisOps.size());
        Assert.assertEquals("a1:3", redisOps.get(0).getOpGtid());
        Assert.assertEquals("a1:15", redisOps.get(12).getOpGtid());
    }

    @Test
    public void testReadRightBoundOpenSegment() throws Exception {
        CommandFileOffsetGtidIndex startIndex = new CommandFileOffsetGtidIndex(new GtidSet("a1:1-16,b1:1-5"),
                new CommandFile(new File("./src/test/resources/GtidSetCommandReaderTest/cmd_1"), 0),
                312);
        Mockito.when(commandStore.findNextFile(startIndex.getCommandFile().getFile()))
                .thenReturn(new CommandFile(new File("./src/test/resources/GtidSetCommandReaderTest/cmd_2"), 0));
        GtidCmdOneSegmentReader reader = new GtidCmdOneSegmentReader(commandStore,
                new CommandFileSegment(startIndex, null), new ArrayParser(), parser);

        List<RedisOp> redisOps = new ArrayList<>();
        while(!reader.isFinish()) {
            RedisOp redisOp = reader.read();
            if (null != redisOp) redisOps.add(redisOp);
        }

        Assert.assertEquals(14, redisOps.size());
        Assert.assertEquals("a1:17", redisOps.get(0).getOpGtid());
        Assert.assertEquals("a1:30", redisOps.get(13).getOpGtid());
    }

}
