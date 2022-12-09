package com.ctrip.xpipe.redis.meta.server.keeper.applier;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * @author ayq
 * <p>
 * 2022/4/17 21:23
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultApplierStateChangeHandlerTest extends AbstractMetaServerTest {

    private static final String sid = "a1";

    private DefaultApplierStateChangeHandler handler;

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private DcMetaCache dcMetaCache;

    private Long clusterDbId, shardDbId;

    private List<ApplierMeta> appliers;

    private RedisMeta redis;

    private List<RedisMeta> redisList;

    private Pair<String, Integer> applierMaster;

    private int setStateTimeMilli = 1500;

    private AtomicInteger calledCount = new AtomicInteger();
    private AtomicInteger redisCall = new AtomicInteger();

    @Before
    public void beforeDefaultApplierStateChangeHandlerTest() throws Exception {
        handler = new DefaultApplierStateChangeHandler();
        handler.setClientPool(getXpipeNettyClientKeyedObjectPool());
        handler.setCurrentMetaManager(currentMetaManager);
        handler.setDcMetaCache(dcMetaCache);
        handler.setScheduled(scheduled);
        handler.setExecutors(executors);

        clusterDbId = getClusterDbId();
        shardDbId = getShardDbId();

        appliers = createRandomAppliers(2);
        redis = newRandomFakeRedisMeta("localhost", randomPort());
        redisList = Collections.singletonList(redis);

        applierMaster = new Pair<>("localhost", randomPort());

        LifecycleHelper.initializeIfPossible(handler);
        LifecycleHelper.startIfPossible(handler);
        add(handler);

        startServer(appliers.get(0).getPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                int current = calledCount.incrementAndGet();
                logger.info("applier0, callCount:{}, msg:{}", current, s);
                sleep(setStateTimeMilli);
                return "+OK\r\n";
            }
        });

        startServer(appliers.get(1).getPort(), new Function<String, String>() {

            @Override
            public String apply(String s) {
                int current = calledCount.incrementAndGet();
                logger.info("applier1, callCount:{}, msg:{}", current, s);
                return "+OK\r\n";
            }
        });

        startServer(redis.getPort(), new Callable<String>() {
            @Override
            public String call() throws Exception {
                redisCall.incrementAndGet();
                return "+OK\r\n";
            }
        });

        when(currentMetaManager.getSurviveAppliers(clusterDbId, shardDbId)).thenReturn(appliers);
        when(currentMetaManager.getApplierMaster(clusterDbId, shardDbId)).thenReturn(applierMaster);
        when(currentMetaManager.getGtidSet(clusterDbId, "a1")).thenReturn(new GtidSet(""));
    }

    @Test
    public void testDifferentShardExecute() throws Exception{

        Long clusterDbId1 = clusterDbId + 1;
        Long shardDbId1 = shardDbId + 1;

        List<ApplierMeta> newAppliers = Lists.newArrayList(MetaClone.clone(appliers.get(1)).setActive(true));
        when(currentMetaManager.getSurviveAppliers(clusterDbId1, shardDbId1)).thenReturn(newAppliers);
        when(currentMetaManager.getApplierMaster(clusterDbId1, shardDbId1)).thenReturn(applierMaster);
        when(currentMetaManager.getGtidSet(clusterDbId1, sid)).thenReturn(new GtidSet(""));


        handler.applierActiveElected(clusterDbId, shardDbId, null, sid);
        handler.applierActiveElected(clusterDbId1, shardDbId1, null, sid);

        sleep(setStateTimeMilli/2);
        Assert.assertEquals(2, calledCount.get());
    }

    @Test
    public void testSameShardExecute() throws Exception {

        handler.applierActiveElected(clusterDbId, shardDbId, null, sid);
        handler.applierActiveElected(clusterDbId, shardDbId, null, sid);
        waitConditionUntilTimeOut(() -> calledCount.get() >= 1);
        sleep(setStateTimeMilli * 3 / 2);
        Assert.assertEquals(2, calledCount.get());
    }
}