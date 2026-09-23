package com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid;

import com.ctrip.xpipe.command.CommandChainException;
import com.ctrip.xpipe.command.DefaultCommandFuture;
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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * InfoReplId 的链式检查：两条 INFO 命令按序执行，五个终止分支、编号新旧、以及 stop 后的丢弃。
 *
 * 测试只替换 session 的异步回调边界（{@link RedisSession#infoReplication}）并显式触发回调，
 * 命令、链、推进逻辑全部走真实实现；不依赖真实网络与真实定时。
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

    /** Two threads: a check stuck on one must not starve a later check. */
    private ExecutorService executors;

    private InfoReplIdAction action;

    /** One callback per started check, in start order. */
    private final List<Callbackable<String>> slaveCallbacks = new CopyOnWriteArrayList<>();

    private final List<Callbackable<String>> keeperCallbacks = new CopyOnWriteArrayList<>();

    @Before
    public void before() throws Exception {
        scheduled = Executors.newSingleThreadScheduledExecutor();
        executors = Executors.newFixedThreadPool(2);

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
        captureInfoCallbacks();

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

    /** 只替换 session 的异步边界：按发起顺序抓住每次都回调，由测试决定何时完成。 */
    private void captureInfoCallbacks() {
        Mockito.doAnswer(invocation -> {
            slaveCallbacks.add(invocation.getArgument(0));
            return null;
        }).when(redisSession).infoReplication(Mockito.any());

        Mockito.doAnswer(invocation -> {
            keeperCallbacks.add(invocation.getArgument(0));
            return null;
        }).when(keeperSession).infoReplication(Mockito.any());
    }

    // ---------- 成功路径 ----------

    @Test
    public void successWhenSlaveReplIdMatchesKeeper() {
        runCheck(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()),
                redisInfo(SLAVE_REPL_ID, null, null, -1));

        InfoReplIdActionContext context = verifyNotified();
        Assert.assertTrue(context.isSuccess());
        Triple<String, String, String> replIds = context.getResult();
        Assert.assertEquals(SLAVE_REPL_ID, replIds.getFirst());
        Assert.assertEquals(SLAVE_REPL_ID, replIds.getMiddle());
    }

    @Test
    public void successWhenSlaveReplIdMatchesKeeperReplId2() {
        runCheck(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()),
                redisInfo(KEEPER_REPL_ID, SLAVE_REPL_ID, null, -1));

        InfoReplIdActionContext context = verifyNotified();
        Assert.assertTrue(context.isSuccess());
        Assert.assertEquals(SLAVE_REPL_ID, context.getResult().getLast());
    }

    @Test
    public void replIdMismatchIsStillASuccessfulContext() {
        runCheck(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()),
                redisInfo(KEEPER_REPL_ID, null, null, -1));

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
        Mockito.verifyNoInteractions(keeperSession);
    }

    @Test
    public void failureWhenSlaveMasterPortMissing() {
        runSlave(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), -1));

        assertFailure(IllegalStateException.class, "slave info incomplete");
        Mockito.verifyNoInteractions(keeperSession);
    }

    @Test
    public void failureWhenKeeperNotInMeta() {
        Mockito.when(metaCache.getKeeperOfDcClusterShard(DC_ID, CLUSTER_ID, SHARD_ID))
                .thenReturn(Collections.singletonList(new KeeperMeta().setIp("10.9.9.9").setPort(6380)));

        runSlave(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()));

        // 监听方按具体类型分支：KeeperNotInMetaException 要告警，不能被链异常包住
        assertFailure(KeeperNotInMetaException.class, null);
        Mockito.verifyNoInteractions(keeperSession);
    }

    @Test
    public void failureWhenKeeperInfoIncomplete() {
        runCheck(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()),
                redisInfo(null, null, null, -1));

        assertFailure(IllegalStateException.class, "keeper info incomplete");
    }

    @Test
    public void failureWhenSlaveCommandFails() {
        action.doTask();
        awaitStageOne(1);
        slaveCallbacks.get(0).fail(new RuntimeException("slave boom"));

        assertFailure(RuntimeException.class, "slave boom");
        Mockito.verifyNoInteractions(keeperSession);
    }

    @Test
    public void failureWhenKeeperCommandFails() {
        runSlave(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()));
        awaitStageTwo(1);
        keeperCallbacks.get(0).fail(new RuntimeException("keeper boom"));

        assertFailure(RuntimeException.class, "keeper boom");
    }

    // ---------- 链的推进 ----------

    @Test
    public void stageTwoIsNotIssuedBeforeStageOneCompletes() {
        action.doTask();
        awaitStageOne(1);

        // 链是顺序的：stage 1 未完成前不得发起 stage 2
        Mockito.verifyNoInteractions(keeperSession);
    }

    @Test
    public void stageTwoIsSkippedWhenStageOneFails() {
        action.doTask();
        awaitStageOne(1);
        slaveCallbacks.get(0).fail(new RuntimeException("timeout"));
        verifyNotified();

        // SequenceCommandChain 失败即停：不得发起 keeper 查询
        Mockito.verifyNoInteractions(keeperSession);
    }

    // ---------- 编号新旧（无 in-flight 守卫的两个方向）----------

    @Test
    public void everyTickStartsItsOwnCheckWithoutWaitingForThePrevious() {
        action.doTask();
        action.doTask();

        // 没有守卫：上一轮未完成也照常发起新一轮
        Mockito.verify(redisSession, Mockito.timeout(2000).times(2)).infoReplication(Mockito.any());
    }

    @Test
    public void aCheckSlowerThanTheTickIntervalStillPublishes() {
        // 模拟「单次检查耗时跨过下一个 tick」：check 1 尚未完成，check 2 已发起
        action.doTask();
        awaitStageOne(1);
        action.doTask();

        // 让 check 1 完整跑完
        slaveCallbacks.get(0).success(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()));
        awaitStageTwo(1);
        keeperCallbacks.get(0).success(redisInfo(SLAVE_REPL_ID, null, null, -1));

        // 慢检查若被一律丢弃，则「持续慢」= 永久静默
        Assert.assertTrue(verifyNotified().isSuccess());
    }

    @Test
    public void lateResultIsDroppedAfterANewerOneWasPublished() {
        action.doTask();
        long olderStamp = action.latestCheckStamp();
        action.doTask();   // 更新的一轮已发起

        // 较新的一轮先产出结果
        action.publish(action.latestCheckStamp(), successChain(), successResult());
        verifyNotified();

        // 较旧的一轮这时才回来：这才是「后回来」，必须丢弃
        action.publish(olderStamp, successChain(), successResult());

        Mockito.verify(listener, Mockito.after(300).times(1)).onAction(Mockito.any());
    }

    @Test
    public void aCheckThatNeverCompletesDoesNotMuteLaterChecks() throws Exception {
        CountDownLatch blocker = new CountDownLatch(1);
        CountDownLatch firstCheckBlocked = new CountDownLatch(1);
        AtomicInteger metaCalls = new AtomicInteger();
        Mockito.when(metaCache.getKeeperOfDcClusterShard(DC_ID, CLUSTER_ID, SHARD_ID))
                .thenAnswer(invocation -> {
                    if (metaCalls.incrementAndGet() == 1) {
                        firstCheckBlocked.countDown();
                        blocker.await();   // 第一次检查永久卡在 stage 2 的 meta 查询上
                    }
                    return Collections.singletonList(
                            new KeeperMeta().setIp(KEEPER.getHost()).setPort(KEEPER.getPort()));
                });

        try {
            action.doTask();
            awaitStageOne(1);
            slaveCallbacks.get(0).success(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()));
            Assert.assertTrue("check 1 未进入 stage 2", firstCheckBlocked.await(2, TimeUnit.SECONDS));

            // 卡住期间下一轮照常发起并完成 —— 这条正是去掉守卫要换来的性质
            action.doTask();
            awaitStageOne(2);
            slaveCallbacks.get(1).success(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()));
            awaitStageTwo(1);
            keeperCallbacks.get(0).success(redisInfo(SLAVE_REPL_ID, null, null, -1));

            Assert.assertTrue("卡住的检查不得让实例静默", verifyNotified().isSuccess());
        } finally {
            blocker.countDown();
        }
    }

    @Test
    public void notificationIsDroppedAfterStop() throws Exception {
        LifecycleHelper.stopIfPossible(action);

        runCheck(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()),
                redisInfo(SLAVE_REPL_ID, null, null, -1));

        Mockito.verify(listener, Mockito.after(300).never()).onAction(Mockito.any());
    }

    // ---------- stage 2 未被执行的收尾 ----------

    @Test
    public void keeperStageFutureIsClosedWhenTheChainFailsBeforeItRuns() {
        // -1 不可能是任何真实编号（从 1 开始），于是走「陈旧丢弃」分支，断言只针对 future 本身
        DefaultCommandFuture<String> slaveFuture = new DefaultCommandFuture<>();
        KeeperReplIdCommand keeperStage = new KeeperReplIdCommand(instance, slaveFuture,
                crossRegionKeeperSessionManager, metaCache, executors);
        DefaultCommandFuture<Object> chainFuture = new DefaultCommandFuture<>();
        chainFuture.setFailure(new CommandChainException("sequence chain, fail stop",
                new RuntimeException("slave boom"), Collections.emptyList()));

        action.publish(-1L, chainFuture, keeperStage.future());

        Assert.assertTrue("未被执行的 stage 的 future 也必须有终态", keeperStage.future().isDone());
        Assert.assertTrue(keeperStage.future().cause() instanceof IllegalStateException);
        Assert.assertEquals("slave boom", keeperStage.future().cause().getCause().getMessage());
    }

    @Test
    public void keeperStageFutureIsNotTouchedWhenThatStageItselfFailed() {
        DefaultCommandFuture<String> slaveFuture = new DefaultCommandFuture<>();
        KeeperReplIdCommand keeperStage = new KeeperReplIdCommand(instance, slaveFuture,
                crossRegionKeeperSessionManager, metaCache, executors);
        keeperStage.future().setFailure(new KeeperNotInMetaException("keeper not in meta"));
        DefaultCommandFuture<Object> chainFuture = new DefaultCommandFuture<>();
        chainFuture.setFailure(new CommandChainException("sequence chain, fail stop",
                new KeeperNotInMetaException("keeper not in meta"), Collections.emptyList()));

        action.publish(-1L, chainFuture, keeperStage.future());

        Assert.assertEquals("keeper not in meta", keeperStage.future().cause().getMessage());
    }

    // ---------- 线程契约 ----------

    @Test
    public void keeperLookupRunsOffTheCompletingThread() {
        // stage 1 的回调由 netty event loop 触发，而解析 / meta 查询必须在池线程上
        Thread completingThread = Thread.currentThread();
        List<Thread> lookupThreads = new CopyOnWriteArrayList<>();
        Mockito.when(metaCache.getKeeperOfDcClusterShard(DC_ID, CLUSTER_ID, SHARD_ID))
                .thenAnswer(invocation -> {
                    lookupThreads.add(Thread.currentThread());
                    return Collections.singletonList(
                            new KeeperMeta().setIp(KEEPER.getHost()).setPort(KEEPER.getPort()));
                });

        runCheck(redisInfo(SLAVE_REPL_ID, null, KEEPER.getHost(), KEEPER.getPort()),
                redisInfo(SLAVE_REPL_ID, null, null, -1));
        verifyNotified();

        Assert.assertFalse("meta 查询未执行", lookupThreads.isEmpty());
        for (Thread lookupThread : lookupThreads) {
            Assert.assertNotSame("meta 查询必须离开完成线程，否则会阻塞 netty event loop",
                    completingThread, lookupThread);
        }
    }

    // ---------- helpers ----------

    /** 一次成功收尾所需的两个 future：链的与 stage 2 结果的。 */
    private DefaultCommandFuture<Object> successChain() {
        DefaultCommandFuture<Object> chain = new DefaultCommandFuture<>();
        chain.setSuccess(Collections.emptyList());
        return chain;
    }

    private DefaultCommandFuture<Triple<String, String, String>> successResult() {
        DefaultCommandFuture<Triple<String, String, String>> result = new DefaultCommandFuture<>();
        result.setSuccess(new Triple<>("a", "a", null));
        return result;
    }

    /** 一轮完整检查：只有 stage 1 完成后，链才会发起 stage 2。 */
    private void runCheck(String slaveInfo, String keeperInfo) {
        runSlave(slaveInfo);
        awaitStageTwo(1);
        keeperCallbacks.get(0).success(keeperInfo);
    }

    private void runSlave(String slaveInfo) {
        action.doTask();
        awaitStageOne(1);
        slaveCallbacks.get(0).success(slaveInfo);
    }

    private void awaitStageOne(int times) {
        Mockito.verify(redisSession, Mockito.timeout(2000).times(times)).infoReplication(Mockito.any());
    }

    private void awaitStageTwo(int times) {
        Mockito.verify(keeperSession, Mockito.timeout(2000).times(times)).infoReplication(Mockito.any());
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

    private String redisInfo(String replId, String replId2, String masterHost, int masterPort) {
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
        return info.toString();
    }
}
