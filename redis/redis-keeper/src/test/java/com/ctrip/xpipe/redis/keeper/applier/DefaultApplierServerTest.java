package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.sync.DefaultCommandDispatcher;
import com.ctrip.xpipe.redis.keeper.applier.threshold.*;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.concurrent.ThreadPoolExecutor;

import static com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 09:14
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultApplierServerTest extends AbstractRedisOpParserTest {

    private ApplierMeta applierMeta;

    @Mock
    private LeaderElectorManager leaderElectorManager;

    @Mock
    private LeaderElector leaderElector;

    @Test
    public void testInit() throws Exception {
        applierMeta = new ApplierMeta().setPort(randomPort());
        when(leaderElectorManager.createLeaderElector(any())).thenReturn(leaderElector);

        DefaultApplierServer server = new DefaultApplierServer(
                "ApplierTest", ClusterId.from(1L), ShardId.from(1L),
                applierMeta, leaderElectorManager, parser, new TestKeeperConfig());
        server.initialize();

        assertTrue(server.sequenceController.getLifecycleState().isInitialized());

        assertNotNull(server.client);
        assertNotNull(server.parser);

        assertEquals(server.parser, ((DefaultCommandDispatcher) server.dispatcher).parser);
        assertEquals(server.sequenceController, ((DefaultCommandDispatcher) server.dispatcher).sequenceController);

        assertEquals(server.gtid_executed, ((DefaultCommandDispatcher) server.dispatcher).gtid_executed);

        assertEquals(server.stateThread, ((DefaultSequenceController) server.sequenceController).stateThread);
        assertEquals(server.workerThreads, ((DefaultSequenceController) server.sequenceController).workerThreads);

        //default 1
        assertEquals(1, ((ThreadPoolExecutor) server.stateThread).getCorePoolSize());
        assertEquals(1, ((ThreadPoolExecutor) server.workerThreads).getCorePoolSize());
        assertEquals(server.scheduled, ((DefaultSequenceController) server.sequenceController).scheduled);

        Field qpsThresholdField = DefaultSequenceController.class.getDeclaredField("qpsThreshold");
        Field concurrencyThresholdField = DefaultSequenceController.class.getDeclaredField("concurrencyThreshold");
        Field bytesPerSecondThresholdField = DefaultSequenceController.class.getDeclaredField("bytesPerSecondThreshold");
        Field memoryThresholdField = DefaultSequenceController.class.getDeclaredField("memoryThreshold");
        qpsThresholdField.setAccessible(true);
        concurrencyThresholdField.setAccessible(true);
        bytesPerSecondThresholdField.setAccessible(true);
        memoryThresholdField.setAccessible(true);

        QPSThreshold qpsThreshold = (QPSThreshold) qpsThresholdField.get(server.sequenceController);
        ConcurrencyThreshold concurrencyThreshold = (ConcurrencyThreshold) concurrencyThresholdField.get(server.sequenceController);
        BytesPerSecondThreshold bytesPerSecondThreshold = (BytesPerSecondThreshold) bytesPerSecondThresholdField.get(server.sequenceController);
        MemoryThreshold memoryThreshold = (MemoryThreshold) memoryThresholdField.get(server.sequenceController);

        Field limitField = AbstractThreshold.class.getDeclaredField("limit");
        limitField.setAccessible(true);
        assertEquals(DEFAULT_QPS_THRESHOLD, limitField.get(qpsThreshold));
        assertEquals(DEFAULT_CONCURRENCY_THRESHOLD, limitField.get(concurrencyThreshold));
        assertEquals(DEFAULT_BYTES_PER_SECOND_THRESHOLD, limitField.get(bytesPerSecondThreshold));
        assertEquals(DEFAULT_MEMORY_THRESHOLD, limitField.get(memoryThreshold));

        assertEquals(server.gtidDistanceThreshold, ((DefaultCommandDispatcher) server.dispatcher).gtidDistanceThreshold);

        assertEquals(server.offsetRecorder, ((DefaultCommandDispatcher) server.dispatcher).offsetRecorder);
        assertEquals(server.offsetRecorder, ((DefaultSequenceController) server.sequenceController).offsetRecorder);

        server.dispose();

        //server.client.close()
    }

    @Test
    public void testParam() throws Exception {
        applierMeta = new ApplierMeta().setPort(randomPort());
        when(leaderElectorManager.createLeaderElector(any())).thenReturn(leaderElector);

        DefaultApplierServer server = new DefaultApplierServer(
                "ApplierTestParam", ClusterId.from(1L), ShardId.from(1L),
                applierMeta, leaderElectorManager, parser, new TestKeeperConfig(),
                1,8,
                50000L, 50000L,50000L,50000L,null);
        server.initialize();

        assertTrue(server.sequenceController.getLifecycleState().isInitialized());

        assertNotNull(server.client);
        assertNotNull(server.parser);

        assertEquals(server.parser, ((DefaultCommandDispatcher) server.dispatcher).parser);
        assertEquals(server.sequenceController, ((DefaultCommandDispatcher) server.dispatcher).sequenceController);

        assertEquals(server.gtid_executed, ((DefaultCommandDispatcher) server.dispatcher).gtid_executed);

        assertEquals(server.stateThread, ((DefaultSequenceController) server.sequenceController).stateThread);
        assertEquals(server.workerThreads, ((DefaultSequenceController) server.sequenceController).workerThreads);

        //default 1
        assertEquals(1, ((ThreadPoolExecutor) server.stateThread).getCorePoolSize());
        assertEquals(8, ((ThreadPoolExecutor) server.workerThreads).getCorePoolSize());
        assertEquals(server.scheduled, ((DefaultSequenceController) server.sequenceController).scheduled);

        Field qpsThresholdField = DefaultSequenceController.class.getDeclaredField("qpsThreshold");
        Field concurrencyThresholdField = DefaultSequenceController.class.getDeclaredField("concurrencyThreshold");
        Field bytesPerSecondThresholdField = DefaultSequenceController.class.getDeclaredField("bytesPerSecondThreshold");
        Field memoryThresholdField = DefaultSequenceController.class.getDeclaredField("memoryThreshold");
        qpsThresholdField.setAccessible(true);
        concurrencyThresholdField.setAccessible(true);
        bytesPerSecondThresholdField.setAccessible(true);
        memoryThresholdField.setAccessible(true);

        QPSThreshold qpsThreshold = (QPSThreshold) qpsThresholdField.get(server.sequenceController);
        ConcurrencyThreshold concurrencyThreshold = (ConcurrencyThreshold) concurrencyThresholdField.get(server.sequenceController);
        BytesPerSecondThreshold bytesPerSecondThreshold = (BytesPerSecondThreshold) bytesPerSecondThresholdField.get(server.sequenceController);
        MemoryThreshold memoryThreshold = (MemoryThreshold) memoryThresholdField.get(server.sequenceController);

        Field limitField = AbstractThreshold.class.getDeclaredField("limit");
        limitField.setAccessible(true);
        assertEquals(50000L, limitField.get(qpsThreshold));
        assertEquals(50000L, limitField.get(concurrencyThreshold));
        assertEquals(50000L, limitField.get(bytesPerSecondThreshold));
        assertEquals(50000L, limitField.get(memoryThreshold));


        assertEquals(server.gtidDistanceThreshold, ((DefaultCommandDispatcher) server.dispatcher).gtidDistanceThreshold);

        assertEquals(server.offsetRecorder, ((DefaultCommandDispatcher) server.dispatcher).offsetRecorder);
        assertEquals(server.offsetRecorder, ((DefaultSequenceController) server.sequenceController).offsetRecorder);

        server.dispose();

        //server.client.close()
    }


    @Test
    public void testHealthCheck() throws Exception {
        applierMeta = new ApplierMeta().setPort(randomPort());
        when(leaderElectorManager.createLeaderElector(any())).thenReturn(leaderElector);

        DefaultApplierServer server = new DefaultApplierServer(
                "ApplierTest", ClusterId.from(1L), ShardId.from(1L),
                applierMeta, leaderElectorManager, parser, new TestKeeperConfig());

        ApplierConfig config = new ApplierConfig();
        config.setDropAllowKeys(1000);
        config.setDropAllowRation(10);
        ApplierStatistic statistic = new ApplierStatistic();
        statistic.setDroppedKeys(5);
        statistic.setTransKeys(20);
        server.setState(ApplierServer.STATE.ACTIVE, config, statistic);
        Assert.assertTrue(server.checkHealth().isHealthy());

        statistic.setTransKeys(100);
        Assert.assertTrue(server.checkHealth().isHealthy());

        statistic.setDroppedKeys(100);
        ApplierServer.ApplierHealth health = server.checkHealth();
        Assert.assertFalse(health.isHealthy());
        Assert.assertEquals("DROP_RATION", health.getCause());

        statistic.setTransKeys(10000000);
        statistic.setDroppedKeys(1001);
        health = server.checkHealth();
        Assert.assertFalse(health.isHealthy());
        Assert.assertEquals("DROP_KEYS", health.getCause());

        server.setState(ApplierServer.STATE.BACKUP, config, statistic);
        Assert.assertTrue(server.checkHealth().isHealthy());
    }
}