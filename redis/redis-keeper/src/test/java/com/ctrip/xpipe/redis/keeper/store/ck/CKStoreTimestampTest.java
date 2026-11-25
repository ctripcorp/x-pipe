package com.ctrip.xpipe.redis.keeper.store.ck;

import com.ctrip.xpipe.api.kafka.GtidKeyItem;
import com.ctrip.xpipe.api.kafka.GtidTimestampItem;
import com.ctrip.xpipe.api.kafka.KafkaService;
import com.ctrip.xpipe.gtid.GtidSet;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class CKStoreTimestampTest {

    private static final String UUID = "000000000000000000000000000000000000000A";

    private RedisOpParser parser;
    private CKStore ckStore;
    private KafkaService kafkaService;
    private final AtomicLong clock = new AtomicLong(119L);
    private final AtomicReference<GtidSet> executed = new AtomicReference<>();

    @Before
    public void setUp() {
        RedisOpParserManager manager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(manager);
        parser = new GeneralRedisOpParser(manager);
        ckStore = new CKStore(ReplId.from(1L), parser, "", new DefaultKeeperConfig());
        ckStore.setSecondsClock(clock::get);
        ckStore.setExecutedGtidSetSupplier(executed::get);
        kafkaService = Mockito.mock(KafkaService.class);
    }

    @After
    public void tearDown() {
        ckStore.dispose();
    }

    @Test
    public void shouldSeedWatermarkOnStart() throws Exception {
        executed.set(new GtidSet(UUID + ":1-100"));
        start();

        ckStore.sendPayloads(Collections.singletonList(set(50)));
        Mockito.verify(kafkaService, Mockito.timeout(15000)).sendKafka(Mockito.any(GtidKeyItem.class));
        ckStore.dispose();

        GtidTimestampItem item = singleTimestampItem();
        Assert.assertEquals(60L, item.getTimestamp());
        Assert.assertEquals(100L, item.getEndSeq());
    }

    @Test
    public void shouldNotSeedOnSetMaster() throws Exception {
        executed.set(new GtidSet(UUID + ":1-10"));
        start();

        // setMaster 时队列可能有积压，播种会把旧分钟的 end_seq 写大，因此不能播种
        executed.set(new GtidSet(UUID + ":1-1000"));
        ckStore.setMaster();

        ckStore.sendPayloads(Collections.singletonList(set(20)));
        Mockito.verify(kafkaService, Mockito.timeout(15000)).sendKafka(Mockito.any(GtidKeyItem.class));
        ckStore.dispose();

        Assert.assertEquals(20L, singleTimestampItem().getEndSeq());
    }

    @Test
    public void shouldKeepConsumingAndFlushAfterSetKeeper() throws Exception {
        start();

        ckStore.sendPayloads(Collections.singletonList(set(7)));
        ckStore.setKeeper();
        Mockito.verify(kafkaService, Mockito.timeout(15000)).sendKafka(Mockito.any(GtidKeyItem.class));
        ckStore.dispose();

        Assert.assertEquals(7L, singleTimestampItem().getEndSeq());
    }

    @Test
    public void shouldFlushExpiredMinuteOnConsumerThread() throws Exception {
        start();

        ckStore.sendPayloads(Collections.singletonList(set(1)));
        Mockito.verify(kafkaService, Mockito.timeout(15000)).sendKafka(Mockito.any(GtidKeyItem.class));

        clock.set(200L);
        ckStore.sendPayloads(Collections.singletonList(set(2)));

        ArgumentCaptor<GtidTimestampItem> captor = ArgumentCaptor.forClass(GtidTimestampItem.class);
        Mockito.verify(kafkaService, Mockito.timeout(15000)).sendTimestampKafka(captor.capture());
        Assert.assertEquals(60L, captor.getValue().getTimestamp());
        Assert.assertEquals(1L, captor.getValue().getEndSeq());
    }

    @Test
    public void shouldSendKeyWhenSeqIsInvalid() throws Exception {
        start();

        ckStore.sendPayloads(Collections.singletonList(
                parse("GTID", UUID + ":abc", "0", "SET", "FOO", "BAR")));
        ArgumentCaptor<GtidKeyItem> captor = ArgumentCaptor.forClass(GtidKeyItem.class);
        Mockito.verify(kafkaService, Mockito.timeout(15000)).sendKafka(captor.capture());
        Assert.assertEquals("abc", captor.getValue().getSeq().toString());

        ckStore.dispose();
        Mockito.verify(kafkaService, Mockito.never()).sendTimestampKafka(Mockito.any());
    }

    private void start() throws Exception {
        ckStore.start();
        Field field = CKStore.class.getDeclaredField("kafkaService");
        field.setAccessible(true);
        field.set(ckStore, kafkaService);
    }

    private GtidTimestampItem singleTimestampItem() {
        ArgumentCaptor<GtidTimestampItem> captor = ArgumentCaptor.forClass(GtidTimestampItem.class);
        Mockito.verify(kafkaService, Mockito.atLeastOnce()).sendTimestampKafka(captor.capture());
        List<GtidTimestampItem> items = captor.getAllValues();
        Assert.assertEquals(1, items.size());
        Assert.assertEquals(UUID, items.get(0).getUuid().toString());
        Assert.assertEquals(1, items.get(0).getShardid());
        return items.get(0);
    }

    private RedisOpItem set(long seq) {
        return parse("GTID", UUID + ":" + seq, "0", "SET", "FOO", "BAR");
    }

    private RedisOpItem parse(String... args) {
        Object[] payload = new Object[args.length];
        try {
            for (int i = 0; i < args.length; i++) {
                DirectByteBufInOutPayload p = new DirectByteBufInOutPayload();
                p.startInput();
                p.in(Unpooled.wrappedBuffer(args[i].getBytes()));
                payload[i] = p;
            }
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
        return RedisOpItemParser.parse(parser, payload);
    }
}
