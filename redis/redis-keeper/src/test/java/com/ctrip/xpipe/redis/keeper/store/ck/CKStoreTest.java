package com.ctrip.xpipe.redis.keeper.store.ck;

import com.ctrip.xpipe.api.kafka.GtidKeyItem;
import com.ctrip.xpipe.api.kafka.KafkaService;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.metric.DummyMetricProxy;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.store.GtidReplicationStore;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RunWith(MockitoJUnitRunner.class)
public class CKStoreTest extends AbstractRedisKeeperTest {

    private CKStore ckStore;
    private RedisOpParser redisOpParser;
    private KeeperConfig keeperConfig;
    private File baseDir;
    private ReplicationStore store;
    private String uuid = "000000000000000000000000000000000000000A";
    private String replId = "000000000000000000000000000000000000000A";
    private CKStore originStore;

    @Before
    public void beforeDefaultReplicationStoreTest() throws IOException, IllegalAccessException, NoSuchFieldException {
        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        redisOpParser = new GeneralRedisOpParser(redisOpParserManager);
        baseDir = new File(getTestFileDir());
        keeperConfig = new DefaultKeeperConfig();
        originStore = new CKStore(ReplId.from(1L), redisOpParser, "", keeperConfig);
        originStore.start(); // 启动消费者线程

        // ========== 改动点：不再创建 Disruptor，直接注入 mock 的 KafkaService、MetricProxy 并启动 CKStore ==========
        KafkaService kafkaService = Mockito.mock(KafkaService.class);
        Field kafkaServiceField = CKStore.class.getDeclaredField("kafkaService");
        kafkaServiceField.setAccessible(true);
        kafkaServiceField.set(originStore, kafkaService);

        MetricProxy metricProxy = new DummyMetricProxy();
        Field metricProxyField = CKStore.class.getDeclaredField("metricProxy");
        metricProxyField.setAccessible(true);
        metricProxyField.set(originStore, metricProxy);

        // ======================================================================================

        this.ckStore = Mockito.spy(originStore);
        store = new GtidReplicationStore(this.ckStore, baseDir, keeperConfig, randomKeeperRunid(), createkeeperMonitor(), redisOpParser, Mockito.mock(SyncRateManager.class));
    }

    @Test
    public void testStopWriteCkSwitch() {
        Assert.assertEquals(false, originStore.checkStopWriteCk());
        ((DefaultKeeperConfig) keeperConfig).onChange(KeeperConfig.KEY_STOP_WRITE_CK, "false", "true");
        sleep(1000);
        Assert.assertEquals(true, originStore.checkStopWriteCk());

        ((DefaultKeeperConfig) keeperConfig).onChange(KeeperConfig.KEY_STOP_WRITE_CK, "true", "false");
        sleep(1000);
        Assert.assertEquals(false, originStore.checkStopWriteCk());
    }

    @Test
    public void testCKStoreWriteCKTest() throws Exception {
        RdbStore rdbStore = store.prepareRdb(replId, 0, new LenEofType(100), ReplStage.ReplProto.XSYNC, new GtidSet(GtidSet.EMPTY_GTIDSET), uuid);
        rdbStore.updateRdbType(RdbStore.Type.NORMAL);
        rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
        store.confirmRdbGapAllowed(rdbStore);

        // 捕获 Kafka 发送
        ArgumentCaptor<GtidKeyItem> captor = ArgumentCaptor.forClass(GtidKeyItem.class);
        CountDownLatch latch = new CountDownLatch(1);

        KafkaService kafkaService = getKafkaService();
        Mockito.doAnswer(invocation -> {
            latch.countDown();
            return true;
        }).when(kafkaService).sendKafka(captor.capture());

        store.appendCommands(Unpooled.wrappedBuffer(generateGtidCommands(uuid, 1, 1)));

        boolean processed = latch.await(10, TimeUnit.SECONDS);
        Assert.assertTrue("Event should be processed within timeout", processed);

        GtidKeyItem item = captor.getValue();
        Assert.assertEquals("FOO", intArrToStr(item.getKey()));
        Assert.assertEquals("SET", item.getCmd());
        Assert.assertEquals(uuid, item.getUuid());
        Assert.assertEquals("1", item.getSeq());
    }

    @Test
    public void testCKStoreMultiWriteCKTest() throws Exception {
        RdbStore rdbStore = store.prepareRdb(replId, 0, new LenEofType(100), ReplStage.ReplProto.XSYNC, new GtidSet(GtidSet.EMPTY_GTIDSET), uuid);
        rdbStore.updateRdbType(RdbStore.Type.NORMAL);
        rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
        store.confirmRdbGapAllowed(rdbStore);

        ArgumentCaptor<GtidKeyItem> captor = ArgumentCaptor.forClass(GtidKeyItem.class);
        // 事务中有两个命令：SET FOO 和 HSET HFOO HGOO hBAR → 理论会产生 2 条 Kafka 消息（SET 产生 1 条，HSET 产生 1 条）
        CountDownLatch latch = new CountDownLatch(2);

        KafkaService kafkaService = getKafkaService();
        Mockito.doAnswer(invocation -> {
            latch.countDown();
            return true;
        }).when(kafkaService).sendKafka(captor.capture());

        store.appendCommands(Unpooled.wrappedBuffer(generateMultiCommands(uuid, 1, 1)));
        store.appendCommands(Unpooled.wrappedBuffer(generateNormalSetCommands(uuid, 1, 1)));
        store.appendCommands(Unpooled.wrappedBuffer(generateNormalHSetCommands(uuid, 1, 1)));
        store.appendCommands(Unpooled.wrappedBuffer(generateExecGtidCommands(uuid, 1, 1)));

        boolean processed = latch.await(10, TimeUnit.SECONDS);
        Assert.assertTrue("Transaction events should be processed within timeout", processed);

        List<GtidKeyItem> items = captor.getAllValues();
        Assert.assertEquals(2, items.size());

        // 验证 SET 命令
        GtidKeyItem setItem = items.stream().filter(i -> "SET".equals(i.getCmd())).findFirst().orElse(null);
        Assert.assertNotNull(setItem);
        Assert.assertEquals("FOO", intArrToStr(setItem.getKey()));
        Assert.assertNull(setItem.getSubkey());

        // 验证 HSET 命令
        GtidKeyItem hsetItem = items.stream().filter(i -> "HSET".equals(i.getCmd())).findFirst().orElse(null);
        Assert.assertNotNull(hsetItem);
        Assert.assertEquals("HFOO", intArrToStr(hsetItem.getKey()));
        Assert.assertEquals("HGOO", intArrToStr(hsetItem.getSubkey()));
    }

    // -------------------- 辅助方法与原测试保持一致 --------------------
    private KafkaService getKafkaService() throws NoSuchFieldException, IllegalAccessException {
        Field field = CKStore.class.getDeclaredField("kafkaService");
        field.setAccessible(true);
        return (KafkaService) field.get(originStore);
    }

    private byte[] generateGtidCommands(String uuid, long startGno, int cmdCount) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        for (int i = 0; i < cmdCount; i++) {
            String uuidGno = uuid + ":" + (startGno + i);
            os.write("*6\r\n".getBytes());
            os.write("$4\r\nGTID\r\n".getBytes());
            os.write('$');
            os.write(String.valueOf(uuidGno.length()).getBytes());
            os.write("\r\n".getBytes());
            os.write(uuidGno.getBytes());
            os.write("\r\n".getBytes());
            os.write("$1\r\n0\r\n".getBytes());
            os.write("$3\r\nSET\r\n".getBytes());
            os.write("$3\r\nFOO\r\n".getBytes());
            os.write("$3\r\nBAR\r\n".getBytes());
        }
        return os.toByteArray();
    }

    private byte[] generateMultiCommands(String uuid, long startGno, int cmdCount) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        for (int i = 0; i < cmdCount; i++) {
            os.write("*1\r\n".getBytes());
            os.write("$5\r\nMULTI\r\n".getBytes());
        }
        return os.toByteArray();
    }

    private byte[] generateNormalSetCommands(String uuid, long startGno, int cmdCount) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        for (int i = 0; i < cmdCount; i++) {
            os.write("*3\r\n".getBytes());
            os.write("$3\r\nSET\r\n".getBytes());
            os.write("$3\r\nFOO\r\n".getBytes());
            os.write("$3\r\nBAR\r\n".getBytes());
        }
        return os.toByteArray();
    }

    private byte[] generateNormalHSetCommands(String uuid, long startGno, int cmdCount) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        for (int i = 0; i < cmdCount; i++) {
            os.write("*4\r\n".getBytes());
            os.write("$4\r\nHSET\r\n".getBytes());
            os.write("$4\r\nHFOO\r\n".getBytes());
            os.write("$4\r\nHGOO\r\n".getBytes());
            os.write("$4\r\nhBAR\r\n".getBytes());
        }
        return os.toByteArray();
    }

    private byte[] generateExecGtidCommands(String uuid, long startGno, int cmdCount) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        for (int i = 0; i < cmdCount; i++) {
            String uuidGno = uuid + ":" + (startGno + i);
            os.write("*4\r\n".getBytes());
            os.write("$4\r\nGTID\r\n".getBytes());
            os.write('$');
            os.write(String.valueOf(uuidGno.length()).getBytes());
            os.write("\r\n".getBytes());
            os.write(uuidGno.getBytes());
            os.write("\r\n".getBytes());
            os.write("$1\r\n0\r\n".getBytes());
            os.write("$4\r\nEXEC\r\n".getBytes());
        }
        return os.toByteArray();
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String intArrToStr(int[] key){
        byte[] str = new byte[key.length];
        for(int i = 0;i<key.length;i++){
            str[i] = (byte) key[i];
        }
        return new String(str);
    }
}