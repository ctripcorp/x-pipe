package com.ctrip.xpipe.redis.keeper.store.ck;

import com.ctrip.xpipe.api.kafka.GtidKeyItem;
import com.ctrip.xpipe.api.kafka.GtidTimestampItem;
import com.ctrip.xpipe.api.kafka.KafkaService;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
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
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CKStoreTimestampTest {

    private static final String UUID = "000000000000000000000000000000000000000A";

    private RedisOpParser parser;
    private CKStore ckStore;
    private KafkaService kafkaService;
    private final AtomicLong clock = new AtomicLong(119L);

    @Before
    public void setUp() {
        RedisOpParserManager manager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(manager);
        parser = new GeneralRedisOpParser(manager);
        ckStore = new CKStore(ReplId.from(1L), parser, "", new DefaultKeeperConfig());
        ckStore.setSecondsClock(clock::get);
        kafkaService = Mockito.mock(KafkaService.class);
    }

    @After
    public void tearDown() {
        ckStore.dispose();
    }

    @Test
    public void shouldAggregateKeyEventAndFlushOnDispose() throws Exception {
        start();

        ckStore.sendPayloads(Collections.singletonList(set(50)));
        Mockito.verify(kafkaService, Mockito.timeout(15000)).sendKafka(Mockito.any(GtidKeyItem.class));
        ckStore.dispose();

        GtidTimestampItem item = singleTimestampItem();
        Assert.assertEquals(60L, item.getTimestamp());
        Assert.assertEquals(50L, item.getEndSeq());
    }

    @Test
    public void shouldReportBlockMetricPerTableWhenSendFails() throws Exception {
        start();
        MetricProxy metricProxy = Mockito.mock(MetricProxy.class);
        setField("metricProxy", metricProxy);
        Mockito.when(kafkaService.initSuccess()).thenReturn(true);
        Mockito.when(kafkaService.timestampInitSuccess()).thenReturn(true);

        // key 发送成功，时间表发送失败
        Mockito.when(kafkaService.sendKafka(Mockito.any())).thenReturn(true);
        Mockito.when(kafkaService.sendTimestampKafka(Mockito.any())).thenReturn(false);

        ckStore.sendPayloads(Collections.singletonList(set(1)));
        Mockito.verify(kafkaService, Mockito.timeout(15000)).sendKafka(Mockito.any(GtidKeyItem.class));
        ckStore.dispose();
        Mockito.verify(kafkaService, Mockito.atLeastOnce()).sendTimestampKafka(Mockito.any());

        reportHickwallNow();

        Map<String, Integer> values = reportedValues(metricProxy);
        Assert.assertEquals(Integer.valueOf(0), values.get("ck.block|key"));
        Assert.assertEquals(Integer.valueOf(1), values.get("ck.block|timestamp"));
        Assert.assertEquals(Integer.valueOf(0), values.get("ck.init|key"));
        Assert.assertEquals(Integer.valueOf(0), values.get("ck.init|timestamp"));

        // 上报后标志复位，再次上报应为 0
        Mockito.reset(metricProxy);
        reportHickwallNow();
        Assert.assertEquals(Integer.valueOf(0), reportedValues(metricProxy).get("ck.block|timestamp"));
    }

    @Test
    public void shouldReportKeyBlockWhenKeySendFails() throws Exception {
        start();
        MetricProxy metricProxy = Mockito.mock(MetricProxy.class);
        setField("metricProxy", metricProxy);
        Mockito.when(kafkaService.sendKafka(Mockito.any())).thenReturn(false);
        Mockito.when(kafkaService.sendTimestampKafka(Mockito.any())).thenReturn(true);

        ckStore.sendPayloads(Collections.singletonList(set(1)));
        Mockito.verify(kafkaService, Mockito.timeout(15000)).sendKafka(Mockito.any(GtidKeyItem.class));

        reportHickwallNow();

        Map<String, Integer> values = reportedValues(metricProxy);
        Assert.assertEquals(Integer.valueOf(1), values.get("ck.block|key"));
        Assert.assertEquals(Integer.valueOf(0), values.get("ck.block|timestamp"));
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
        setField("kafkaService", kafkaService);
    }

    private void setField(String name, Object value) throws Exception {
        Field field = CKStore.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(ckStore, value);
    }

    /** 直接执行与定时任务相同的上报逻辑，避免等待 1 分钟调度 */
    private void reportHickwallNow() throws Exception {
        Method method = CKStore.class.getDeclaredMethod("reportHickwall", String.class, String.class, boolean.class);
        method.setAccessible(true);
        Field keyFail = CKStore.class.getDeclaredField("isSendCkFail");
        Field tsFail = CKStore.class.getDeclaredField("isSendTimestampFail");
        keyFail.setAccessible(true);
        tsFail.setAccessible(true);

        method.invoke(ckStore, "ck.block", "key", keyFail.getBoolean(ckStore));
        method.invoke(ckStore, "ck.init", "key", !kafkaService.initSuccess());
        keyFail.setBoolean(ckStore, false);
        method.invoke(ckStore, "ck.block", "timestamp", tsFail.getBoolean(ckStore));
        method.invoke(ckStore, "ck.init", "timestamp", !kafkaService.timestampInitSuccess());
        tsFail.setBoolean(ckStore, false);
    }

    /** 收集 metricName|tableTag -> value */
    private Map<String, Integer> reportedValues(MetricProxy metricProxy) throws Exception {
        ArgumentCaptor<MetricData> captor = ArgumentCaptor.forClass(MetricData.class);
        Mockito.verify(metricProxy, Mockito.atLeastOnce()).writeBinMultiDataPoint(captor.capture());
        Map<String, Integer> values = new HashMap<>();
        for (MetricData data : captor.getAllValues()) {
            values.put(data.getMetricType() + "|" + data.getTags().get("table"), (int) data.getValue());
        }
        return values;
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
