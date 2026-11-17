package com.ctrip.xpipe.redis.keeper.applier.sync;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisSingleKeyOpGtidWrapper;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.keeper.applier.command.DefaultDataCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpCommand;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.threshold.GTIDDistanceThreshold;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import redis.clients.util.SafeEncoder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * @author Slight
 * <p>
 * Oct 10, 2022 15:56
 */
public class DefaultCommandDispatcherTest {

    DefaultCommandDispatcher dispatcher = new DefaultCommandDispatcher();

    @Before
    public void setUp() throws Exception {
        dispatcher.stateThread = MoreExecutors.newDirectExecutorService();
        dispatcher.gtidDistanceThreshold = new AtomicReference<>(new GTIDDistanceThreshold(2000));
        ReflectionTestUtils.setField(dispatcher, "execGtidSet", new AtomicReference<GtidSet>());
        dispatcher.resetState();

    }

    @Test
    public void toInt() {
        for (int i = 0; i < 257; i++) {

            byte[] bytes = SafeEncoder.encode(i + "");

            int rt = dispatcher.toInt(bytes);
            assertEquals(i, rt);
        }

    }

    @Test
    public void testFilterPublish() {
        RedisSingleKeyOp op1 = new RedisOpSingleKey(RedisOpType.SET, string2Bytes("set a 1"), null, null);
        RedisOp gtidOp1 = new RedisSingleKeyOpGtidWrapper(string2Bytes("GTID ggg:1 0"), "ggg","", op1);

        Assert.assertFalse(dispatcher.shouldFilter(gtidOp1));

        RedisSingleKeyOp op2 = new RedisOpSingleKey(RedisOpType.PUBLISH, string2Bytes("publish xpipe-asymmetric-ppp 222"), null, null);
        RedisOp gtidOp2 = new RedisSingleKeyOpGtidWrapper(string2Bytes("GTID ggg:1 0"), "ggg","", op2);
        Assert.assertFalse(dispatcher.shouldFilter(gtidOp2));

        RedisSingleKeyOp op3 = new RedisOpSingleKey(RedisOpType.PUBLISH, string2Bytes("publish ppp 222"), null, null);
        RedisOp gtidOp3 = new RedisSingleKeyOpGtidWrapper(string2Bytes("GTID ggg:1 0"), "ggg", "",op3);
        Assert.assertTrue(dispatcher.shouldFilter(gtidOp3));
        Assert.assertTrue(dispatcher.shouldFilter(op3));

        //test estimated size by the way
        Assert.assertEquals(15, gtidOp1.estimatedSize());
        Assert.assertEquals(40, gtidOp2.estimatedSize());
        Assert.assertEquals(23, gtidOp3.estimatedSize());
    }

    @Test
    public void testGtidCommandUsesGtidDbId() {
        ApplierSequenceController sequenceController = configureDispatcherForCommandDispatch();

        dispatcher.doOnAppendCommand(resp("SELECT", "0"));
        dispatcher.doOnAppendCommand(resp("GTID", "0123456789012345678901234567890123456789:1", "15", "SET", "key", "value"));

        ArgumentCaptor<RedisOpCommand> commandCaptor = ArgumentCaptor.forClass(RedisOpCommand.class);
        Mockito.verify(sequenceController).submit(commandCaptor.capture(), Mockito.anyLong(), Mockito.any(GtidSet.class));
        DefaultDataCommand command = (DefaultDataCommand) commandCaptor.getValue();
        Assert.assertEquals(15, command.getDbNumber());
        Assert.assertArrayEquals(string2Bytes("SET key value"), command.redisOp().buildRawOpArgs());
    }

    private ApplierSequenceController configureDispatcherForCommandDispatch() {
        ApplierSequenceController sequenceController = Mockito.mock(ApplierSequenceController.class);
        RedisOpParserManager parserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(parserManager);
        dispatcher.sequenceController = sequenceController;
        dispatcher.client = Mockito.mock(AsyncRedisClient.class);
        dispatcher.parser = new GeneralRedisOpParser(parserManager);
        dispatcher.offsetRecorder = new AtomicLong();
        return sequenceController;
    }

    private ByteBuf resp(String... arguments) {
        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeBytes(("*" + arguments.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
        for (String argument : arguments) {
            byte[] bytes = argument.getBytes(StandardCharsets.US_ASCII);
            byteBuf.writeBytes(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
            byteBuf.writeBytes(bytes);
            byteBuf.writeBytes("\r\n".getBytes(StandardCharsets.US_ASCII));
        }
        return byteBuf;
    }

    private byte[][] string2Bytes(String s) {

        String[] ss = s.split(" ");
        int length = ss.length;
        byte[][] b = new byte[length][];

        for (int i = 0; i < length; i++) {
            b[i] = ss[i].getBytes();
        }

        return b;
    }
}