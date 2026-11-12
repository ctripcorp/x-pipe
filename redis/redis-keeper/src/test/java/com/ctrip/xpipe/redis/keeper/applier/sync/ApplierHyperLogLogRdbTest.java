package com.ctrip.xpipe.redis.keeper.applier.sync;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.client.redis.DoNothingRedisClient;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.DefaultRdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.DefaultRdbParser;
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.ctrip.xpipe.redis.core.redis.rdb.parser.RdbDataBytes.hllKeys;
import static com.ctrip.xpipe.redis.core.redis.rdb.parser.RdbDataBytes.hllPayloads;

public class ApplierHyperLogLogRdbTest {

    private DefaultCommandDispatcher dispatcher;
    private ApplierSequenceController sequenceController;
    private RecordingRedisClient client;

    @Before
    public void setUp() {
        dispatcher = new DefaultCommandDispatcher();
        sequenceController = Mockito.mock(ApplierSequenceController.class);
        client = new RecordingRedisClient();
        dispatcher.sequenceController = sequenceController;
        dispatcher.client = client;
        dispatcher.stateThread = MoreExecutors.newDirectExecutorService();
        dispatcher.gtidDistanceThreshold = new AtomicReference<>(new GTIDDistanceThreshold(2000));
        dispatcher.execGtidSet = new AtomicReference<>();
        dispatcher.offsetRecorder = new AtomicLong();
        dispatcher.resetState();
    }

    @Test
    public void testApplierForwardsRedis820HyperLogLogRdbPayload() throws Throwable {
        DefaultRdbParser rdbParser = new DefaultRdbParser(new DefaultRdbParseContext());
        rdbParser.registerListener(dispatcher);
        ByteBuf rdb = Unpooled.wrappedBuffer(realHllRdb());
        while (!rdbParser.isFinish()) {
            rdbParser.read(rdb);
        }

        ArgumentCaptor<RedisOpCommand> commandCaptor = ArgumentCaptor.forClass(RedisOpCommand.class);
        Mockito.verify(sequenceController, Mockito.times(hllKeys.length))
                .submit(commandCaptor.capture(), Mockito.anyLong(), Mockito.any(GtidSet.class));

        List<RedisOpCommand> commands = commandCaptor.getAllValues();
        for (int i = 0; i < hllKeys.length; i++) {
            DefaultDataCommand command = (DefaultDataCommand) commands.get(i);
            Assert.assertEquals(0, command.getDbNumber());
            Assert.assertArrayEquals("SET".getBytes(StandardCharsets.US_ASCII), command.redisOp().buildRawOpArgs()[0]);
            Assert.assertArrayEquals(hllKeys[i].getBytes(StandardCharsets.US_ASCII), command.redisOp().buildRawOpArgs()[1]);
            Assert.assertArrayEquals(hllPayloads[i], command.redisOp().buildRawOpArgs()[2]);
            command.execute().get();
        }

        Assert.assertEquals(hllKeys.length, client.writes.size());
        for (int i = 0; i < hllKeys.length; i++) {
            RecordedWrite write = client.writes.get(i);
            Assert.assertEquals(0, write.dbNumber);
            Assert.assertArrayEquals("SET".getBytes(StandardCharsets.US_ASCII), (byte[]) write.rawArgs[0]);
            Assert.assertArrayEquals(hllKeys[i].getBytes(StandardCharsets.US_ASCII), (byte[]) write.rawArgs[1]);
            Assert.assertArrayEquals(hllPayloads[i], (byte[]) write.rawArgs[2]);
        }
    }

    private byte[] realHllRdb() throws IOException {
        try (InputStream input = getClass().getResourceAsStream("/applier/hll-redis-820.rdb")) {
            Assert.assertNotNull("HLL RDB fixture not found", input);
            return input.readAllBytes();
        }
    }

    private static class RecordingRedisClient extends DoNothingRedisClient {

        private List<RecordedWrite> writes = new ArrayList<>();

        @Override
        public CommandFuture<Object> write(Object resource, int dbNumber, Object... rawArgs) {
            writes.add(new RecordedWrite(dbNumber, rawArgs));
            return super.write(resource, dbNumber, rawArgs);
        }
    }

    private static class RecordedWrite {

        private final int dbNumber;
        private final Object[] rawArgs;

        private RecordedWrite(int dbNumber, Object[] rawArgs) {
            this.dbNumber = dbNumber;
            this.rawArgs = rawArgs;
        }
    }
}
