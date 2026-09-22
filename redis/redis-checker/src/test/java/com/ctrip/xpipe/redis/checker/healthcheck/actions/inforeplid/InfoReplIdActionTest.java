package com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.checker.healthcheck.session.CrossRegionKeeperSessionManager;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.unidal.tuple.Triple;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * InfoReplId 的异步链路：五个终止分支、防重入、以及 stop 后的回调丢弃。
 *
 * 两个 INFO 命令都由测试显式触发回调，不依赖真实网络与真实定时。
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class InfoReplIdActionTest {

    private static final String CLUSTER_ID = "cluster1";
    private static final String SHARD_ID = "shard1";
    private static final String DC_ID = "fra";

    private static final HostPort REDIS = new HostPort("10.0.1.1", 6379);
    private static final HostPort KEEPER = new HostPort("10.0.2.1", 6380);

    private static final String SLAVE_REPL_ID = "replid-slave";
    private static final String KEEPER_REPL_ID = "replid-keeper";

    @Mock
    private RedisHealthCheckInstance instance;

    @Mock
    private RedisInstanceInfo instanceInfo;

    @Mock
    private RedisSession redisSession;

    @Mock
    private RedisSession keeperSession;

    @Mock
    private CrossRegionKeeperSessionManager crossRegionKeeperSessionManager;

    @Mock
    private MetaCache metaCache;

    @Mock
    private HealthCheckConfig healthCheckConfig;

    @Mock
    private InfoReplIdActionListener listener;

    private ScheduledExecutorService scheduled;

    private ExecutorService executors;

    private InfoReplIdAction action;

    /** Written from the dispatch executor thread (stage 2 is reached off the callback thread). */
    private volatile Callbackable<InfoResultExtractor> slaveCallback;

    private volatile Callbackable<InfoResultExtractor> keeperCallback;

    @Before
    public void before() throws Exception {
        scheduled = Executors.newSingleThreadScheduledExecutor();
        executors = Executors.newSingleThreadExecutor();

        Mockito.when(instance.getCheckInfo()).thenReturn(instanceInfo);
        Mockito.when(instance.getRedisSession()).thenReturn(redisSession);
        Mockito.when(instance.getHealthCheckConfig()).thenReturn(healthCheckConfig);
        // 远大于测试时长：周期任务只由测试显式调用 doTask 驱动
        Mockito.when(healthCheckConfig.checkIntervalMilli()).thenReturn(60_000);
        Mockito.when(instanceInfo.getHostPort()).thenReturn(REDIS);
        Mockito.when(instanceInfo.getDcId()).thenReturn(DC_ID);
        Mockito.when(instanceInfo.getClusterId()).thenReturn(CLUSTER_ID);
        Mockito.when(instanceInfo.getShardId()).thenReturn(SHARD_ID);

        Mockito.when(listener.worksfor(Mockito.any())).thenReturn(true);
        Mockito.when(crossRegionKeeperSessionManager.findOrCreateSession(KEEPER)).thenReturn(keeperSession);
        Mockito.when(metaCache.getKeeperOfDcClusterShard(DC_ID, CLUSTER_ID, SHARD_ID))
                .thenReturn(Collections.singletonList(
                        new KeeperMeta().setIp(KEEPER.getHost()).setPort(KEEPER.getPort())));

        captureAsyncCommands();

        action = new InfoReplIdAction(scheduled, instance, executors,
                crossRegionKeeperSessionManager, metaCache);
        action.addListener(listener);
        LifecycleHelper.initializeIfPossible(action);
        LifecycleHelper.startIfPossible(action);
    }

    @After
    public void after() throws Exception {
        LifecycleHelper.stopIfPossible(action);
        scheduled.shutdownNow();
        executors.shutdownNow();
    }

    @SuppressWarnings("unchecked")
    private void captureAsyncCommands() {
        Mockito.doAnswer(invocation -> {
            slaveCallback = invocation.getArgument(1);
            return null;
        }).when(redisSession).info(Mockito.eq(InfoCommand.INFO_TYPE.REPLICATION), Mockito.any());

        Mockito.doAnswer(invocation -> {
            keeperCallback = invocation.getArgument(1);
            return null;
        }).when(keeperSession).info(Mockito.eq(InfoCommand.INFO_TYPE.REPLICATION), Mockito.any());
    }

    // ---------- 成功路径 ----------

    @Test
    public void successWhenSlaveReplIdMatchesKeeper() {
        runSlave(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()));
        runKeeper(redisInfo(SLAVE_REPL_ID, null, null, -1));

        InfoReplIdActionContext context = verifyNotified();
        Assert.assertTrue(context.isSuccess());
        Triple<String, String, String> replIds = context.getResult();
        Assert.assertEquals(SLAVE_REPL_ID, replIds.getFirst());
        Assert.assertEquals(SLAVE_REPL_ID, replIds.getMiddle());
    }

    @Test
    public void successWhenSlaveReplIdMatchesKeeperReplId2() {
        runSlave(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()));
        runKeeper(redisInfo(KEEPER_REPL_ID, SLAVE_REPL_ID, null, -1));

        InfoReplIdActionContext context = verifyNotified();
        Assert.assertTrue(context.isSuccess());
        Assert.assertEquals(SLAVE_REPL_ID, context.getResult().getLast());
    }

    @Test
    public void replIdMismatchIsStillASuccessfulContext() {
        runSlave(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()));
        runKeeper(redisInfo(KEEPER_REPL_ID, null, null, -1));

        // 匹配与否由 CrossRegionRedisHealthStatus.replIdMatch 判定，action 只负责把三个 replId 送出去
        InfoReplIdActionContext context = verifyNotified();
        Assert.assertTrue(context.isSuccess());
        Assert.assertEquals(KEEPER_REPL_ID, context.getResult().getMiddle());
        Assert.assertNull(context.getResult().getLast());
    }

    // ---------- 失败分支 ----------

    @Test
    public void failureWhenSlaveInfoIncomplete() {
        runSlave(redisInfo(null, null, KEEPER.getHost(), KEEPER.getPort()));

        assertFailure(IllegalStateException.class, "slave info incomplete");
        Mockito.verifyNoInteractions(crossRegionKeeperSessionManager);
    }

    @Test
    public void failureWhenSlaveMasterPortMissing() {
        runSlave(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), -1));

        assertFailure(IllegalStateException.class, "slave info incomplete");
        Mockito.verifyNoInteractions(crossRegionKeeperSessionManager);
    }

    @Test
    public void failureWhenKeeperNotInMeta() {
        Mockito.when(metaCache.getKeeperOfDcClusterShard(DC_ID, CLUSTER_ID, SHARD_ID))
                .thenReturn(Collections.singletonList(new KeeperMeta().setIp("10.9.9.9").setPort(6380)));

        runSlave(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()));

        assertFailure(KeeperNotInMetaException.class, null);
        // 连错 keeper 时不得再去读它的 INFO
        Mockito.verifyNoInteractions(crossRegionKeeperSessionManager);
    }

    @Test
    public void failureWhenKeeperInfoIncomplete() {
        runSlave(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()));
        runKeeper(redisInfo(null, null, null, -1));

        assertFailure(IllegalStateException.class, "keeper info incomplete");
    }

    @Test
    public void failureWhenSlaveCommandFails() {
        action.doTask();
        slaveCallback.fail(new RuntimeException("slave boom"));

        InfoReplIdActionContext context = verifyNotified();
        Assert.assertFalse(context.isSuccess());
        Assert.assertEquals("slave boom", context.getCause().getMessage());
    }

    @Test
    public void failureWhenKeeperCommandFails() {
        runSlave(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()));
        awaitKeeperCommand();
        keeperCallback.fail(new RuntimeException("keeper boom"));

        InfoReplIdActionContext context = verifyNotified();
        Assert.assertFalse(context.isSuccess());
        Assert.assertEquals("keeper boom", context.getCause().getMessage());
    }

    @Test
    public void exceptionInsideSuccessCallbackIsCapturedNotEscaped() {
        Mockito.when(metaCache.getKeeperOfDcClusterShard(DC_ID, CLUSTER_ID, SHARD_ID))
                .thenThrow(new IllegalStateException("meta boom"));

        // 回调内抛出的异常必须被捕获成失败上下文，而不是逃逸到命令线程
        runSlave(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()));

        InfoReplIdActionContext context = verifyNotified();
        Assert.assertFalse(context.isSuccess());
        Assert.assertEquals("meta boom", context.getCause().getMessage());
    }

    // ---------- 防重入与生命周期 ----------

    @Test
    public void secondTickIsSkippedWhileCheckInFlight() {
        action.doTask();
        action.doTask();

        Mockito.verify(redisSession, Mockito.times(1))
                .info(Mockito.eq(InfoCommand.INFO_TYPE.REPLICATION), Mockito.any());
    }

    @Test
    public void nextTickRunsAfterCompletion() {
        runSlave(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()));
        runKeeper(redisInfo(SLAVE_REPL_ID, null, null, -1));
        verifyNotified();

        action.doTask();

        Mockito.verify(redisSession, Mockito.times(2))
                .info(Mockito.eq(InfoCommand.INFO_TYPE.REPLICATION), Mockito.any());
    }

    @Test
    public void inFlightIsReleasedOnFailurePath() {
        runSlave(redisInfo(null, null, KEEPER.getHost(), KEEPER.getPort()));
        assertFailure(IllegalStateException.class, "slave info incomplete");

        action.doTask();

        // 失败路径同样必须复位，否则该实例会被永久跳过
        Mockito.verify(redisSession, Mockito.times(2))
                .info(Mockito.eq(InfoCommand.INFO_TYPE.REPLICATION), Mockito.any());
    }

    @Test
    public void notificationIsDroppedAfterStop() throws Exception {
        LifecycleHelper.stopIfPossible(action);

        action.doTask();
        slaveCallback.success(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()));
        slaveCallback.fail(new RuntimeException("late"));

        Thread.sleep(200);
        Mockito.verify(listener, Mockito.never()).onAction(Mockito.any());
    }

    // ---------- 线程契约 ----------

    @Test
    public void callbackWorkIsOffloadedFromTheCompletingThread() {
        // 生产环境下 success/fail 由 netty event loop 调用；回调体必须不在这条线程上干活
        Thread completingThread = Thread.currentThread();
        AtomicReference<Thread> workThread = new AtomicReference<>();
        Mockito.when(metaCache.getKeeperOfDcClusterShard(DC_ID, CLUSTER_ID, SHARD_ID))
                .thenAnswer(invocation -> {
                    workThread.set(Thread.currentThread());
                    return Collections.singletonList(
                            new KeeperMeta().setIp(KEEPER.getHost()).setPort(KEEPER.getPort()));
                });

        runSlave(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()));
        runKeeper(redisInfo(SLAVE_REPL_ID, null, null, -1));
        verifyNotified();

        Assert.assertNotNull("meta 查询未执行", workThread.get());
        Assert.assertNotSame("meta 查询必须离开回调线程，否则会阻塞 netty event loop",
                completingThread, workThread.get());
    }

    // ---------- helpers ----------

    private void runSlave(InfoResultExtractor slaveInfo) {
        action.doTask();
        slaveCallback.success(slaveInfo);
    }

    private void runKeeper(InfoResultExtractor keeperInfo) {
        awaitKeeperCommand();
        keeperCallback.success(keeperInfo);
    }

    /**
     * 回调被投递到线程池执行，stage 2 的发起不再同步，必须等它真的发出命令。
     */
    private void awaitKeeperCommand() {
        Mockito.verify(keeperSession, Mockito.timeout(2000).atLeastOnce())
                .info(Mockito.eq(InfoCommand.INFO_TYPE.REPLICATION), Mockito.any());
        Assert.assertNotNull("stage 2 未被触发", keeperCallback);
    }

    private void assertFailure(Class<? extends Throwable> expected, String message) {
        InfoReplIdActionContext context = verifyNotified();
        Assert.assertFalse(context.isSuccess());
        Assert.assertTrue("expected " + expected + " but was " + context.getCause(),
                expected.isInstance(context.getCause()));
        if (message != null) {
            Assert.assertEquals(message, context.getCause().getMessage());
        }
    }

    private InfoReplIdActionContext verifyNotified() {
        ArgumentCaptor<InfoReplIdActionContext> captor = ArgumentCaptor.forClass(InfoReplIdActionContext.class);
        Mockito.verify(listener, Mockito.timeout(2000)).onAction(captor.capture());
        return captor.getValue();
    }

    private InfoResultExtractor redisInfo(String replId, String replId2, String masterHost, int masterPort) {
        StringBuilder info = new StringBuilder();
        info.append("# Replication\r\n");
        info.append("role:slave\r\n");
        if (masterHost != null) {
            info.append("master_host:").append(masterHost).append("\r\n");
        }
        if (masterPort > 0) {
            info.append("master_port:").append(masterPort).append("\r\n");
        }
        if (replId != null) {
            info.append("master_replid:").append(replId).append("\r\n");
        }
        if (replId2 != null) {
            info.append("master_replid2:").append(replId2).append("\r\n");
        }
        return new InfoResultExtractor(info.toString());
    }
}
