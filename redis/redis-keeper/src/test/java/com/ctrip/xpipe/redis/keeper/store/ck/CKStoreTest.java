package com.ctrip.xpipe.redis.keeper.store.ck;

import com.ctrip.xpipe.api.kafka.KafkaService;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.kafka.DefaultKafkaService;
import com.ctrip.xpipe.metric.DummyMetricProxy;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.redis.operation.*;
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
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
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

/**
 * @author TB
 * <p>
 * 2025/10/21 13:44
 */
@RunWith(MockitoJUnitRunner.class)
public class CKStoreTest  extends AbstractRedisKeeperTest {

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
        originStore = new CKStore(ReplId.from(1l),redisOpParser,"",keeperConfig);
        originStore.start();

        MessageEventFactory factory = new MessageEventFactory();
        int ringBufferSize = 1024; // 1M个槽位
        Disruptor<MessageEvent> disruptor = new Disruptor<>(
                factory,                    // 事件工厂
                ringBufferSize,             // RingBuffer大小
                DaemonThreadFactory.INSTANCE,
                ProducerType.SINGLE,
                new SleepingWaitStrategy()
        );

        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            IRedisOpItem iRedisOpItem = event.getRedisOpItem();
            Object item = iRedisOpItem.getRedisOpItem();
            if(item instanceof RedisOpItem){
                ckStore.storeGtidWithKeyOrSubKeyItem((RedisOpItem) item);
            }else {
                List<RedisOpItem> redisOpItems = (List<RedisOpItem>) item;
                for (RedisOpItem redisOpItem : redisOpItems) {
                    ckStore.storeGtidWithKeyOrSubKeyItem(redisOpItem);
                }
            }
        });

        KafkaService kafkaService = new DefaultKafkaService();
        Field  kafkaServiceField = CKStore.class.getDeclaredField("kafkaService");
        kafkaServiceField.setAccessible(true);
        kafkaServiceField.set(originStore,kafkaService);

        MetricProxy metricProxy = new DummyMetricProxy();
        Field  metricProxyField = CKStore.class.getDeclaredField("metricProxy");
        metricProxyField.setAccessible(true);
        metricProxyField.set(originStore,metricProxy);

        RingBuffer ringBuffer = disruptor.start();
        Field disruptorField = CKStore.class.getDeclaredField("disruptor");
        disruptorField.setAccessible(true);
        disruptorField.set(originStore,disruptor);

        Field ringBufferField = CKStore.class.getDeclaredField("ringBuffer");
        ringBufferField.setAccessible(true);
        ringBufferField.set(originStore,ringBuffer);

        this.ckStore = Mockito.spy(originStore);
        store = new GtidReplicationStore(this.ckStore,baseDir, keeperConfig, randomKeeperRunid(), createkeeperMonitor(), redisOpParser, Mockito.mock(SyncRateManager.class));
    }

    @Test
    public void testStopWriteCkSwitch(){
        Assert.assertEquals(false,originStore.checkStopWriteCk());
        ((DefaultKeeperConfig)keeperConfig).onChange(KeeperConfig.KEY_STOP_WRITE_CK,"false","true");
        sleep(1000);
        Assert.assertEquals(true,originStore.checkStopWriteCk());

        ((DefaultKeeperConfig)keeperConfig).onChange(KeeperConfig.KEY_STOP_WRITE_CK,"true","false");
        sleep(1000);
        Assert.assertEquals(false,originStore.checkStopWriteCk());
    }

    @Test
    public void testCKStoreWriteCKTest() throws Exception {
        RdbStore rdbStore = store.prepareRdb(replId, 0, new LenEofType(100), ReplStage.ReplProto.XSYNC, new GtidSet(GtidSet.EMPTY_GTIDSET), uuid);
        rdbStore.updateRdbType(RdbStore.Type.NORMAL);
        rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
        store.confirmRdbGapAllowed(rdbStore);
        ArgumentCaptor<String> gtidCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dbidCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RedisOpItem> redisOpCaptor = ArgumentCaptor.forClass(RedisOpItem.class);

        CountDownLatch latch = new CountDownLatch(1);
        // 设置mock行为，在方法调用时countDown
        Mockito.doAnswer(invocation -> {
            latch.countDown();
            return null; // 或者返回适当的值
        }).when(ckStore).writeCKItem(gtidCaptor.capture(),dbidCaptor.capture(),redisOpCaptor.capture());
        store.appendCommands(Unpooled.wrappedBuffer(generateGtidCommands(uuid,1,1)));
        // Then - 等待事件被处理
        boolean processed = latch.await(10, TimeUnit.SECONDS);
        Assert.assertTrue("Event should be processed within timeout", processed);
        RedisOpItem redisOpItem = (RedisOpItem) redisOpCaptor.getValue();
        Assert.assertEquals("FOO",redisOpItem.getRedisKey().toString());
    }


    @Test
    public void testCKStoreMultiWriteCKTest() throws Exception {
        RdbStore rdbStore = store.prepareRdb(replId, 0, new LenEofType(100), ReplStage.ReplProto.XSYNC, new GtidSet(GtidSet.EMPTY_GTIDSET), uuid);
        rdbStore.updateRdbType(RdbStore.Type.NORMAL);
        rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
        store.confirmRdbGapAllowed(rdbStore);
        ArgumentCaptor<String> gtidCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dbidCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<List> redisOpCaptor = ArgumentCaptor.forClass(List.class);

        CountDownLatch latch = new CountDownLatch(1);
        // 设置mock行为，在方法调用时countDown
        Mockito.doAnswer(invocation -> {
            latch.countDown();
            return null; // 或者返回适当的值
        }).when(ckStore).writeCKItem(gtidCaptor.capture(),dbidCaptor.capture(),redisOpCaptor.capture());
        store.appendCommands(Unpooled.wrappedBuffer(generateMultiCommands(uuid,1,1)));
        store.appendCommands(Unpooled.wrappedBuffer(generateNormalSetCommands(uuid,1,1)));
        store.appendCommands(Unpooled.wrappedBuffer(generateNormalHSetCommands(uuid,1,1)));
        store.appendCommands(Unpooled.wrappedBuffer(generateExecGtidCommands(uuid,1,1)));
        // Then - 等待事件被处理
        boolean processed = latch.await(10, TimeUnit.SECONDS);
        Assert.assertTrue("Event should be processed within timeout", processed);
        RedisOpItem redisOpSingleItem = (RedisOpItem) redisOpCaptor.getValue().get(0);
        RedisOpItem redisMultiSubKeyOpItem = (RedisOpItem) redisOpCaptor.getValue().get(1);
        Assert.assertEquals("FOO",redisOpSingleItem.getRedisKey().toString());
        Assert.assertEquals("HFOO",redisMultiSubKeyOpItem.getRedisKey().toString());
        Assert.assertEquals("HGOO",redisMultiSubKeyOpItem.getRedisKeyList().get(0).toString());
    }

    private byte[] generateGtidCommands(String uuid, long startGno, int cmdCount) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        for (int i = 0; i < cmdCount; i++) {
            String uuidGno = uuid + ":" + String.valueOf(startGno+i);
            os.write("*6\r\n".getBytes());
            os.write("$4\r\nGTID\r\n".getBytes());
            os.write('$'); os.write(String.valueOf(uuidGno.length()).getBytes()); os.write("\r\n".getBytes()); os.write(uuidGno.getBytes()); os.write("\r\n".getBytes());
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
            String uuidGno = uuid + ":" + String.valueOf(startGno+i);
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
            String uuidGno = uuid + ":" + String.valueOf(startGno+i);
            os.write("*4\r\n".getBytes());
            os.write("$4\r\nGTID\r\n".getBytes());
            os.write('$'); os.write(String.valueOf(uuidGno.length()).getBytes()); os.write("\r\n".getBytes()); os.write(uuidGno.getBytes()); os.write("\r\n".getBytes());
            os.write("$1\r\n0\r\n".getBytes());
            os.write("$4\r\nEXEC\r\n".getBytes());
        }
        return os.toByteArray();
    }

}
