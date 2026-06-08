package com.ctrip.xpipe.redis.keeper.store.ck;

import com.ctrip.xpipe.api.kafka.KafkaService;
import com.ctrip.xpipe.payload.DirectByteBufInOutPayload;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpItem;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpItemParser;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CKStoreTransactionBatchTest {

    private CKStore ckStore;
    private RedisOpParser parser;

    @Before
    public void setUp() throws Exception {
        RedisOpParserManager manager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(manager);
        parser = new GeneralRedisOpParser(manager);
        ckStore = new CKStore(ReplId.from(1L), parser, "", new DefaultKeeperConfig());
        ckStore.start();
        KafkaService kafkaService = Mockito.mock(KafkaService.class);
        Field field = CKStore.class.getDeclaredField("kafkaService");
        field.setAccessible(true);
        field.set(ckStore, kafkaService);
    }

    @Test
    public void testTransactionBatchProducesTwoKafkaMessages() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        Field field = CKStore.class.getDeclaredField("kafkaService");
        field.setAccessible(true);
        KafkaService kafkaService = (KafkaService) field.get(ckStore);
        Mockito.doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(kafkaService).sendKafka(Mockito.any());

        List<RedisOpItem> items = new ArrayList<>();
        items.add(parse("MULTI"));
        items.add(parse("SET", "FOO", "BAR"));
        items.add(parse("HSET", "HFOO", "HGOO", "hBAR"));
        items.add(parse("GTID", "000000000000000000000000000000000000000A:1", "0", "EXEC"));
        ckStore.sendPayloads(items);

        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        ckStore.dispose();
    }

    private RedisOpItem parse(String... args) throws Exception {
        Object[] payload = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            DirectByteBufInOutPayload p = new DirectByteBufInOutPayload();
            p.startInput();
            p.in(Unpooled.wrappedBuffer(args[i].getBytes()));
            payload[i] = p;
        }
        return RedisOpItemParser.parse(parser, payload);
    }
}
