package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;
import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.ctrip.xpipe.redis.core.store.CommandFile;
import com.ctrip.xpipe.redis.core.store.CommandFileOffsetGtidIndex;
import com.ctrip.xpipe.redis.core.store.CommandFileSegment;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import com.ctrip.xpipe.utils.OffsetNotifier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.mockito.ArgumentMatchers.any;

/**
 * @author lishanglin
 * date 2022/5/6
 */
@RunWith(MockitoJUnitRunner.class)
public class GtidSetCommandReaderTest extends AbstractRedisOpParserTest {

    @Mock
    private CommandStore<?, RedisOp> commandStore;

    @Mock
    private OffsetNotifier notifier;

    private GtidSet excludedGtidSet = new GtidSet("a1:1-5");

    @Before
    public void setupGtidSetCommandReaderTest() throws Exception {
        Mockito.when(commandStore.simpleDesc()).thenReturn("test");
    }

    @Test
    @Ignore // write mock data info local files
    public void mockData() throws IOException {
        int cmdIndex = 1;
        for (int fileIndex = 0; fileIndex < 3; fileIndex++) {
            ControllableFile controllableFile = null;
            try {
                File cmdFile = new File("./src/test/resources/GtidSetCommandReaderTest/cmd_" + fileIndex);
                controllableFile = new DefaultControllableFile(cmdFile);

                for (int i = 0; i < 10; i++) {
                    logger.info("[mockData][{}][{}] begin at {}", cmdFile, cmdIndex, controllableFile.getFileChannel().position());
                    String rawCmd = mockCmdRaw("a1:" + cmdIndex, "k" + cmdIndex, "v" + cmdIndex);
                    controllableFile.getFileChannel().write(ByteBuffer.wrap(rawCmd.getBytes()));
                    logger.info("[mockData][{}][{}] end at {}", cmdFile, cmdIndex, controllableFile.getFileChannel().position());
                    cmdIndex++;
                }

            } finally {
                if (null != controllableFile) controllableFile.close();
            }
        }
    }

    @Test
    public void testReadCrossFileSegment() throws Exception {
        CommandFileOffsetGtidIndex startIndex = new CommandFileOffsetGtidIndex(new GtidSet("a1:1-2,b1:1-5"),
                new CommandFile(new File("./src/test/resources/GtidSetCommandReaderTest/cmd_0"), 0),
                98);
        CommandFileOffsetGtidIndex endIndex = new CommandFileOffsetGtidIndex(new GtidSet("a1:1-15,b1:1-5"),
                new CommandFile(new File("./src/test/resources/GtidSetCommandReaderTest/cmd_1"), 0),
                260);
        Mockito.when(commandStore.findFirstFileSegment(any())).thenReturn(new CommandFileSegment(startIndex, endIndex));
        Mockito.when(commandStore.findNextFile(startIndex.getCommandFile().getFile())).thenReturn(endIndex.getCommandFile());

        GtidSetCommandReader reader = new GtidSetCommandReader(commandStore, new GtidSet("a1:1-2"), new ArrayParser(), parser, notifier, 100);
        Mockito.verify(commandStore).findFirstFileSegment(any());

        for (int i = 3; i <= 15;) {
            RedisOp redisOp = reader.read();
            if (null == redisOp) continue;

            Assert.assertEquals("a1:" + i, redisOp.getOpGtid());
            Assert.assertArrayEquals(("k" + i).getBytes(), ((RedisSingleKeyOp) redisOp).getKey().get());
            Assert.assertArrayEquals(("v" + i).getBytes(), ((RedisSingleKeyOp) redisOp).getValue());
            Mockito.verify(commandStore).findFirstFileSegment(any());
            logger.info("[testReadCrossFile] check round {} success", i);
            i++;
        }

        reader.read(); // roll to next segment
        Mockito.verify(commandStore, Mockito.times(2)).findFirstFileSegment(any());
    }

    @Test
    public void testReadMultiSegment() throws IOException {
        CommandFileOffsetGtidIndex startIndex = new CommandFileOffsetGtidIndex(new GtidSet("a1:1-5,b1:1-5"),
                new CommandFile(new File("./src/test/resources/GtidSetCommandReaderTest/cmd_0"), 0),
                245);
        CommandFileOffsetGtidIndex endIndex = new CommandFileOffsetGtidIndex(new GtidSet("a1:1-9,b1:1-5"),
                new CommandFile(new File("./src/test/resources/GtidSetCommandReaderTest/cmd_0"), 0),
                441);
        Mockito.when(commandStore.findFirstFileSegment(any())).thenReturn(new CommandFileSegment(startIndex, endIndex));
        Mockito.when(commandStore.findNextFile(startIndex.getCommandFile().getFile()))
                .thenReturn(new CommandFile(new File("./src/test/resources/GtidSetCommandReaderTest/cmd_1"), 0));

        GtidSetCommandReader reader = new GtidSetCommandReader(commandStore, new GtidSet("a1:1-5"), new ArrayParser(), parser, notifier, 100);
        Mockito.verify(commandStore).findFirstFileSegment(any());

        startIndex = endIndex;
        endIndex = new CommandFileOffsetGtidIndex(new GtidSet("a1:1-16,b1:1-5"),
                new CommandFile(new File("./src/test/resources/GtidSetCommandReaderTest/cmd_1"), 0),
                312);
        Mockito.when(commandStore.findFirstFileSegment(any())).thenReturn(new CommandFileSegment(startIndex, endIndex));

        for (int i = 6; i <= 16;) {
            RedisOp redisOp = reader.read();
            if (null == redisOp) continue;

            Assert.assertEquals("a1:" + i, redisOp.getOpGtid());
            Assert.assertArrayEquals(("k" + i).getBytes(), ((RedisSingleKeyOp) redisOp).getKey().get());
            Assert.assertArrayEquals(("v" + i).getBytes(), ((RedisSingleKeyOp) redisOp).getValue());
            logger.info("[testReadCrossFile] check round {} success", i);
            i++;
        }

        Mockito.verify(commandStore, Mockito.times(2)).findFirstFileSegment(any());
    }

    @Test
    public void testReadRightBoundOpenSegment() throws IOException {
        CommandFileOffsetGtidIndex startIndex = new CommandFileOffsetGtidIndex(new GtidSet("a1:1-16,b1:1-5"),
                new CommandFile(new File("./src/test/resources/GtidSetCommandReaderTest/cmd_1"), 0),
                312);
        Mockito.when(commandStore.findFirstFileSegment(any())).thenReturn(new CommandFileSegment(startIndex));
        Mockito.when(commandStore.findNextFile(startIndex.getCommandFile().getFile()))
                .thenReturn(new CommandFile(new File("./src/test/resources/GtidSetCommandReaderTest/cmd_2"), 0));

        GtidSetCommandReader reader = new GtidSetCommandReader(commandStore, new GtidSet("a1:1-16"), new ArrayParser(), parser, notifier, 100);

        for (int i = 17; i <= 30;) {
            RedisOp redisOp = reader.read();
            if (null == redisOp) continue;

            Assert.assertEquals("a1:" + i, redisOp.getOpGtid());
            Assert.assertArrayEquals(("k" + i).getBytes(), ((RedisSingleKeyOp) redisOp).getKey().get());
            Assert.assertArrayEquals(("v" + i).getBytes(), ((RedisSingleKeyOp) redisOp).getValue());
            logger.info("[testReadCrossFile] check round {} success", i);
            i++;
        }
    }

    @Test
    public void testReadFromHugeFilePosition() throws IOException {

    }

    @Test
    public void testReadSkipCarelessCmdInSegmentHead() {

    }

    @Test
    public void testReadSkipCarelessCmdWithinSegment() {

    }

    @Test
    public void testReadSkipCarelessCmdInSegmentTail() {

    }

    private String mockCmdRaw(String gtid, String key, String val) {
        return  "*5\r\n" +
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
