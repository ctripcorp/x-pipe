package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.cluster.LeaderLatchProbeStatus.Reason;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * {@link LeaderLatch} 的包装，补上它缺失的「整体重建」能力与断链探测（keeper-dynamic-ip spec §5.6）。
 * <p>
 * LeaderLatch 是纯事件驱动、无定时器兜底的状态机，background 回调遇到未处理的返回码就断链，
 * 之后 ZK 上没有节点、本地没有 watch，连接保持健康时不会再被唤醒。唯一的出路是丢掉旧实例、
 * 新建一个重新 start。
 * <p>
 * <b>业务代码只持有本类，不得直接持有 LeaderLatch 实例</b>：重建会替换内部实例，
 * 调用方手里的旧引用从那一刻起就是一个已关闭的对象。
 * <p>
 * <b>断链探测</b>：start 后在共享调度器上每 {@code probeIntervalMilli} 探测一次。前提是 latch 为
 * STARTED 且 ZK 连接健康，前提不成立时不下结论、清零连续不健康时长。前提成立时任一条命中即不健康：
 * ① ourPath 为 null ② ourPath 在 ZK 上不存在 ③ ourPath 的 ephemeralOwner 不是当前 session。
 * 连续不健康满 {@code unhealthyThresholdMilli} 才重建。
 * <p>
 * 判据只看「本实例的节点在不在、是不是自己的」。前驱 watch 回调失败（B）或 getChildren 失败（C-其余）
 * 而节点仍在时，丢的只是领导权通知，节点注册完好，这里判为健康。只把 latch 当有序临时注册用的场景
 * （KeeperContainerZkRegistrar）不受影响；若将来复用到依赖 hasLeadership 的选主，需另加领导权判据。
 * <p>
 * 宽限期不可省：LeaderLatch.reset() 先 setNode(null) 再异步 create，ourPath 要等 create 回调才重新赋值。
 * session 过期重连、首节点自检发现 owner 不符等任何走 reset 的路径，ourPath 都会短暂为 null，
 * 没有宽限期必然误判重建。
 * <p>
 * 同步策略：start / close / rebuild / manualRebuild 全部串行在 {@link #lock} 上。定时探测与运维接口
 * 都必须经由 {@link #rebuild(String)}，共用这把锁。没有这把锁，两个并发重建会各自 new 一个 latch，
 * 后写入字段的那个把先写入的覆盖掉，被覆盖的实例永远不会被 close，其临时节点一直挂到 session 结束，
 * 即孤儿节点。探测结果的记账在 {@link #probeLock} 上，锁顺序固定为 lock → probeLock。
 * 状态读取方法不加锁，只读 volatile 字段。
 * <p>
 * 持锁期间调用的 LeaderLatch.close() / start() 都不阻塞在 IO 上：删节点、摘 watch 走 background，
 * start 把建节点交给 AfterConnectionEstablished 的后台线程。探测对 ZK 的查询也走 background，
 * 回调里只记账，需要重建时投递回调度器执行，不在 ZK 事件线程上重建。所以探测与重建可以跑在共享调度线程上。
 * <p>
 * 本类不挂 LeaderLatchListener。将来若要挂，listener 不得回调本类的加锁方法：LeaderLatch 在自身
 * monitor 内同步通知 listener，而重建持本类锁时会去拿 latch 的 monitor，两边交叉即死锁。
 */
public class RebuildableLeaderLatch implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(RebuildableLeaderLatch.class);

    public static final long DEFAULT_PROBE_INTERVAL_MILLI = 10_000L;
    public static final long DEFAULT_UNHEALTHY_THRESHOLD_MILLI = 30_000L;

    public static final String EVENT_TYPE = "LeaderLatch";
    public static final String EVENT_REBUILD = "rebuild";
    public static final String EVENT_REBUILD_CLOSE_FAIL = "rebuild.close.fail";
    public static final String EVENT_REBUILD_START_FAIL = "rebuild.start.fail";
    public static final String EVENT_PROBE_UNHEALTHY = "probe.unhealthy";
    public static final String EVENT_MANUAL_REBUILD = "rebuild.manual";

    public enum ManualRebuildResult {
        REBUILT,
        /** 最近一次探测不是 UNHEALTHY 且未带 force。 */
        REJECTED_NOT_UNHEALTHY,
        /** 未 start 或已 close。 */
        NOT_RUNNING,
        /** 新实例 start 失败。 */
        FAILED
    }

    private final CuratorFramework client;
    private final String latchPath;
    private final String id;
    private final ScheduledExecutorService probeScheduler;
    private final long probeIntervalMilli;
    private final long unhealthyThresholdNanos;

    private final Object lock = new Object();
    private final Object probeLock = new Object();

    /** 写入只在 lock 内；读取不加锁。start 之前为 null，close 之后保留最后一个（已关闭的）实例。 */
    private volatile LeaderLatch latch;

    /** 写入只在 lock 内；探测线程无锁读取，用于尽早丢弃关闭后的结果。 */
    private volatile boolean closed;

    /** 仅在 lock 内读写。 */
    private ScheduledFuture<?> probeFuture;

    /** 写入只在 probeLock 内；读取不加锁。 */
    private volatile LeaderLatchProbeStatus probeStatus;

    /** 仅在 probeLock 内读写；小于 0 表示当前不在连续不健康区间内。 */
    private long unhealthySinceNanos = -1;

    public RebuildableLeaderLatch(CuratorFramework client, String latchPath, String id) {
        this(client, latchPath, id, sharedProbeScheduler());
    }

    public RebuildableLeaderLatch(CuratorFramework client, String latchPath, String id,
                                  ScheduledExecutorService probeScheduler) {
        this(client, latchPath, id, probeScheduler, DEFAULT_PROBE_INTERVAL_MILLI, DEFAULT_UNHEALTHY_THRESHOLD_MILLI);
    }

    public RebuildableLeaderLatch(CuratorFramework client, String latchPath, String id,
                                  ScheduledExecutorService probeScheduler,
                                  long probeIntervalMilli, long unhealthyThresholdMilli) {
        if (probeIntervalMilli <= 0 || unhealthyThresholdMilli <= 0) {
            throw new IllegalArgumentException("[RebuildableLeaderLatch]probe interval and threshold must be positive, "
                    + probeIntervalMilli + ", " + unhealthyThresholdMilli);
        }
        this.client = client;
        this.latchPath = latchPath;
        this.id = id;
        this.probeScheduler = probeScheduler;
        this.probeIntervalMilli = probeIntervalMilli;
        this.unhealthyThresholdNanos = TimeUnit.MILLISECONDS.toNanos(unhealthyThresholdMilli);
        this.probeStatus = newStatus(Reason.NOT_PROBED, LeaderLatch.State.LATENT, null);
    }

    public void start() throws Exception {
        synchronized (lock) {
            if (closed || null != latch) {
                throw new IllegalStateException("[start]cannot be started more than once, " + desc());
            }
            LeaderLatch fresh = newLatch();
            // 先赋值再 start：start 即使抛异常，LeaderLatch 也可能已置为 STARTED，close / rebuild 要能处理掉它
            this.latch = fresh;
            resetProbe(fresh);
            // 探测先于 start 挂上：start 抛异常时 latch 可能卡在 STARTED 却什么都没做，正需要探测兜底
            this.probeFuture = probeScheduler.scheduleWithFixedDelay(this::probe,
                    probeIntervalMilli, probeIntervalMilli, TimeUnit.MILLISECONDS);
            fresh.start();
            logger.info("[start]{}", desc());
        }
    }

    /**
     * 丢弃当前 latch，新建一个并 start（spec §5.6 重建动作）。
     * <p>
     * 旧实例 close 抛异常不阻断重建：LeaderLatch.internalClose 入口先 CAS STARTED→CLOSED，
     * finally 里摘掉连接状态 listener，因此即使 close 抛出，旧实例也已失活，不会再建节点或回调。
     * 删除旧节点走的是 background guaranteed delete，同样在抛出之前就已发起。
     * <p>
     * 重建会让本实例主动放弃领导权：新节点 seq 更大，排到同路径其他参与者之后。
     *
     * @return 新实例已 start 返回 true；本对象未 start、已 close，或新实例 start 失败返回 false
     */
    public boolean rebuild(String reason) {
        synchronized (lock) {
            LeaderLatch old = this.latch;
            if (closed || null == old) {
                logger.warn("[rebuild][skip, not running]{}, closed:{}, reason:{}", desc(), closed, reason);
                return false;
            }
            logger.info("[rebuild][begin]{}, oldPath:{}, oldState:{}, reason:{}",
                    desc(), old.getOurPath(), old.getState(), reason);
            try {
                old.close();
            } catch (Throwable th) {
                logger.error("[rebuild][close old fail, continue]{}, reason:{}", desc(), reason, th);
                EventMonitor.DEFAULT.logError(EVENT_TYPE, EVENT_REBUILD_CLOSE_FAIL);
            }

            LeaderLatch fresh = newLatch();
            this.latch = fresh;
            // 新实例重新计宽限期；旧实例还在路上的探测结果按实例比对后丢弃
            resetProbe(fresh);
            try {
                fresh.start();
            } catch (Throwable th) {
                // 字段已指向 fresh，下一次重建或 close 会把它处理掉
                logger.error("[rebuild][start new fail]{}, reason:{}", desc(), reason, th);
                EventMonitor.DEFAULT.logError(EVENT_TYPE, EVENT_REBUILD_START_FAIL);
                return false;
            }
            logger.info("[rebuild][done]{}, reason:{}", desc(), reason);
            EventMonitor.DEFAULT.logEvent(EVENT_TYPE, EVENT_REBUILD);
            return true;
        }
    }

    /**
     * 运维手工重置（spec §5.6 Ih），复用 {@link #rebuild(String)}。
     * <p>
     * 最近一次探测不是 UNHEALTHY 时必须带 force。重建会让本实例放弃当前排位：旧节点删除、新节点 seq
     * 更大排到队尾。对 KeeperContainerZkRegistrar 而言，同 hostname 下若有其他 pod，消费方会立即改为
     * 解析到对方；对选主场景则是 leader 主动让位。健康实例上误操作的代价就是这一次换位。
     * <p>
     * 检查与重建在同一把锁内完成，不会出现「刚被自动重建为健康，又被一次未带 force 的请求重建」。
     */
    public ManualRebuildResult manualRebuild(boolean force, String reason) {
        synchronized (lock) {
            if (closed || null == latch) {
                logger.warn("[manualRebuild][not running]{}, force:{}, reason:{}", desc(), force, reason);
                return ManualRebuildResult.NOT_RUNNING;
            }
            LeaderLatchProbeStatus status = this.probeStatus;
            if (!force && !status.isUnhealthy()) {
                logger.warn("[manualRebuild][rejected, not unhealthy and no force]{}, reason:{}, status:{}",
                        desc(), reason, status);
                return ManualRebuildResult.REJECTED_NOT_UNHEALTHY;
            }
            logger.warn("[manualRebuild]{}, force:{}, reason:{}, status:{}; rebuild gives up current position, "
                    + "the new node is queued after all other participants", desc(), force, reason, status);
            EventMonitor.DEFAULT.logEvent(EVENT_TYPE, EVENT_MANUAL_REBUILD);
            return rebuild("manual(force=" + force + "): " + reason)
                    ? ManualRebuildResult.REBUILT : ManualRebuildResult.FAILED;
        }
    }

    /**
     * 幂等。close 之后 {@link #rebuild(String)} 不再生效，避免关闭后又被重建出一个无人回收的节点。
     */
    @Override
    public void close() throws IOException {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            if (null != probeFuture) {
                probeFuture.cancel(false);
                probeFuture = null;
            }
            LeaderLatch current = this.latch;
            if (null == current || LeaderLatch.State.STARTED != current.getState()) {
                return;
            }
            current.close();
            logger.info("[close]{}", desc());
        }
    }

    /** 当前实例的状态；未 start 时为 LATENT。 */
    public LeaderLatch.State getState() {
        LeaderLatch current = this.latch;
        return null == current ? LeaderLatch.State.LATENT : current.getState();
    }

    /** 当前实例的节点完整路径，未建好、重建中或已关闭时为 null。 */
    public String getOurPath() {
        LeaderLatch current = this.latch;
        return null == current ? null : current.getOurPath();
    }

    public boolean hasLeadership() {
        LeaderLatch current = this.latch;
        return null != current && current.hasLeadership();
    }

    /** 最近一次探测结果，最多滞后一个探测间隔；start 前与重建后到下一次探测前为 NOT_PROBED。 */
    public LeaderLatchProbeStatus getProbeStatus() {
        return probeStatus;
    }

    public String getLatchPath() {
        return latchPath;
    }

    public String getId() {
        return id;
    }

    /** 探测任务挂靠的调度器，默认为进程内共享的 {@link #sharedProbeScheduler()}。 */
    protected ScheduledExecutorService getProbeScheduler() {
        return probeScheduler;
    }

    @VisibleForTesting
    protected LeaderLatch newLatch() {
        return new LeaderLatch(client, latchPath, id);
    }

    /** 周期任务入口，兜住全部异常，否则 scheduleWithFixedDelay 会静默停掉后续执行。 */
    @VisibleForTesting
    protected void probe() {
        try {
            doProbe();
        } catch (Throwable th) {
            logger.error("[probe]{}", desc(), th);
        }
    }

    private void doProbe() throws Exception {
        LeaderLatch current = this.latch;
        if (closed || null == current) {
            return;
        }
        LeaderLatch.State state = current.getState();
        String ourPath = current.getOurPath();
        if (LeaderLatch.State.STARTED != state) {
            record(current, ourPath, newStatus(Reason.NOT_STARTED, state, ourPath));
            return;
        }
        if (!client.getZookeeperClient().isConnected()) {
            record(current, ourPath, newStatus(Reason.DISCONNECTED, state, ourPath));
            return;
        }
        if (null == ourPath) {
            record(current, null, newStatus(Reason.OUR_PATH_NULL, state, null));
            return;
        }
        // background：不占调度线程；exists 不校验 ACL，latch 父路径的读权限异常也不影响探测
        client.checkExists()
                .inBackground((c, event) -> onCheckExists(current, ourPath, event))
                .forPath(ourPath);
    }

    private void onCheckExists(LeaderLatch probed, String ourPath, CuratorEvent event) {
        try {
            LeaderLatch.State state = probed.getState();
            int rc = event.getResultCode();
            LeaderLatchProbeStatus status;
            if (KeeperException.Code.OK.intValue() == rc) {
                Stat stat = event.getStat();
                long owner = stat.getEphemeralOwner();
                long sessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
                Reason reason = owner == sessionId ? Reason.OK : Reason.OWNER_MISMATCH;
                status = newStatus(reason, state, ourPath).withNode(true, owner, sessionId);
            } else if (KeeperException.Code.NONODE.intValue() == rc) {
                status = newStatus(Reason.PATH_NOT_EXIST, state, ourPath).withNode(false, null, null);
            } else {
                status = newStatus(Reason.PROBE_ERROR, state, ourPath).withResultCode(rc);
            }
            record(probed, ourPath, status);
        } catch (Throwable th) {
            logger.error("[onCheckExists]{}, ourPath:{}", desc(), ourPath, th);
        }
    }

    /**
     * 探测结果记账。结果对应的实例或节点已经变了（重建过、或 latch 自己 reset 过）就丢弃，
     * 不让过期结论影响当前实例的连续不健康时长。
     */
    private void record(LeaderLatch probed, String probedPath, LeaderLatchProbeStatus status) {
        boolean rebuildNeeded = false;
        boolean becameUnhealthy = false;
        long unhealthyMillis = 0;
        synchronized (probeLock) {
            if (closed || probed != this.latch || !Objects.equals(probedPath, probed.getOurPath())) {
                return;
            }
            if (status.isUnhealthy()) {
                long now = System.nanoTime();
                if (unhealthySinceNanos < 0) {
                    unhealthySinceNanos = now;
                    becameUnhealthy = true;
                }
                long unhealthyNanos = now - unhealthySinceNanos;
                unhealthyMillis = TimeUnit.NANOSECONDS.toMillis(unhealthyNanos);
                rebuildNeeded = unhealthyNanos >= unhealthyThresholdNanos;
            } else {
                unhealthySinceNanos = -1;
            }
            this.probeStatus = status.withUnhealthyDuration(unhealthyMillis);
        }

        if (!status.isUnhealthy()) {
            return;
        }
        logger.warn("[probe][unhealthy]{}", status);
        if (becameUnhealthy) {
            EventMonitor.DEFAULT.logEvent(EVENT_TYPE, EVENT_PROBE_UNHEALTHY);
        }
        if (rebuildNeeded) {
            String reason = "probe " + status.getReason() + " for " + unhealthyMillis + "ms";
            try {
                // 可能在 ZK 事件线程上，重建投递回调度器执行
                probeScheduler.execute(() -> rebuildIfCurrent(probed, reason));
            } catch (RejectedExecutionException e) {
                logger.error("[probe][submit rebuild rejected]{}, reason:{}", desc(), reason, e);
            }
        }
    }

    /** 只重建探测时的那个实例；多次投递时，第一次之后实例已换，其余为 no-op。 */
    private void rebuildIfCurrent(LeaderLatch expected, String reason) {
        try {
            synchronized (lock) {
                if (expected != this.latch) {
                    return;
                }
                rebuild(reason);
            }
        } catch (Throwable th) {
            logger.error("[rebuildIfCurrent]{}, reason:{}", desc(), reason, th);
        }
    }

    private void resetProbe(LeaderLatch current) {
        synchronized (probeLock) {
            unhealthySinceNanos = -1;
            this.probeStatus = newStatus(Reason.NOT_PROBED, current.getState(), null);
        }
    }

    private LeaderLatchProbeStatus newStatus(Reason reason, LeaderLatch.State state, String ourPath) {
        return new LeaderLatchProbeStatus(latchPath, id, reason, state, ourPath, System.currentTimeMillis());
    }

    private String desc() {
        return latchPath + "(" + id + ")";
    }

    /**
     * 进程内所有 RebuildableLeaderLatch 共用的探测调度器。
     * <p>
     * 禁止一个 latch 一个 timer：分片级选主（{@link DefaultLeaderElector}）每个分片一个实例，
     * 实例数随分片规模增长。单线程足够，前提是挂在上面的任务不阻塞：探测走 ZK background 调用，
     * 重建本身也不阻塞（见类注释）。
     * <p>
     * 守护线程、不随任何实例关闭；removeOnCancel 保证实例关闭时取消的周期任务不在队列里堆积。
     */
    public static ScheduledExecutorService sharedProbeScheduler() {
        return SharedProbeSchedulerHolder.INSTANCE;
    }

    private static final class SharedProbeSchedulerHolder {
        private static final ScheduledExecutorService INSTANCE = create();

        private static ScheduledExecutorService create() {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1,
                    XpipeThreadFactory.create("LeaderLatchProbe", true));
            executor.setRemoveOnCancelPolicy(true);
            return executor;
        }
    }
}
