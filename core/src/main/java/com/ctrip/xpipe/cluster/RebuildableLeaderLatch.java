package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * {@link LeaderLatch} 的包装，补上它缺失的「整体重建」能力（keeper-dynamic-ip spec §5.6）。
 * <p>
 * LeaderLatch 是纯事件驱动、无定时器兜底的状态机，background 回调遇到未处理的返回码就断链，
 * 之后 ZK 上没有节点、本地没有 watch，连接保持健康时不会再被唤醒。唯一的出路是丢掉旧实例、
 * 新建一个重新 start。
 * <p>
 * <b>业务代码只持有本类，不得直接持有 LeaderLatch 实例</b>：重建会替换内部实例，
 * 调用方手里的旧引用从那一刻起就是一个已关闭的对象。
 * <p>
 * 同步策略：start / close / rebuild 全部串行在 {@link #lock} 上。定时探测（Ig）与运维接口（Ih）
 * 都必须经由 {@link #rebuild(String)}，共用这把锁。没有这把锁，两个并发重建会各自 new 一个 latch，
 * 后写入字段的那个把先写入的覆盖掉，被覆盖的实例永远不会被 close，其临时节点一直挂到 session 结束，
 * 即孤儿节点。状态读取方法不加锁，只读 volatile 字段。
 * <p>
 * 持锁期间调用的 LeaderLatch.close() / start() 都不阻塞在 IO 上：删节点、摘 watch 走 background，
 * start 把建节点交给 AfterConnectionEstablished 的后台线程。所以重建可以跑在共享调度线程上。
 * <p>
 * 本类不挂 LeaderLatchListener。将来若要挂，listener 不得回调本类的加锁方法：LeaderLatch 在自身
 * monitor 内同步通知 listener，而重建持本类锁时会去拿 latch 的 monitor，两边交叉即死锁。
 */
public class RebuildableLeaderLatch implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(RebuildableLeaderLatch.class);

    public static final String EVENT_TYPE = "LeaderLatch";
    public static final String EVENT_REBUILD = "rebuild";
    public static final String EVENT_REBUILD_CLOSE_FAIL = "rebuild.close.fail";
    public static final String EVENT_REBUILD_START_FAIL = "rebuild.start.fail";

    private final CuratorFramework client;
    private final String latchPath;
    private final String id;
    private final ScheduledExecutorService probeScheduler;

    private final Object lock = new Object();

    /** 写入只在 lock 内；读取不加锁。start 之前为 null，close 之后保留最后一个（已关闭的）实例。 */
    private volatile LeaderLatch latch;

    /** 仅在 lock 内读写。 */
    private boolean closed;

    public RebuildableLeaderLatch(CuratorFramework client, String latchPath, String id) {
        this(client, latchPath, id, sharedProbeScheduler());
    }

    public RebuildableLeaderLatch(CuratorFramework client, String latchPath, String id,
                                  ScheduledExecutorService probeScheduler) {
        this.client = client;
        this.latchPath = latchPath;
        this.id = id;
        this.probeScheduler = probeScheduler;
    }

    public void start() throws Exception {
        synchronized (lock) {
            if (closed || null != latch) {
                throw new IllegalStateException("[start]cannot be started more than once, " + desc());
            }
            LeaderLatch fresh = newLatch();
            // 先赋值再 start：start 即使抛异常，LeaderLatch 也可能已置为 STARTED，close / rebuild 要能处理掉它
            this.latch = fresh;
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
     * 幂等。close 之后 {@link #rebuild(String)} 不再生效，避免关闭后又被重建出一个无人回收的节点。
     */
    @Override
    public void close() throws IOException {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
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

    public String getLatchPath() {
        return latchPath;
    }

    public String getId() {
        return id;
    }

    /** 探测任务（Ig）挂靠的调度器，默认为进程内共享的 {@link #sharedProbeScheduler()}。 */
    protected ScheduledExecutorService getProbeScheduler() {
        return probeScheduler;
    }

    @VisibleForTesting
    protected LeaderLatch newLatch() {
        return new LeaderLatch(client, latchPath, id);
    }

    private String desc() {
        return latchPath + "(" + id + ")";
    }

    /**
     * 进程内所有 RebuildableLeaderLatch 共用的探测调度器。
     * <p>
     * 禁止一个 latch 一个 timer：分片级选主（{@link DefaultLeaderElector}）每个分片一个实例，
     * 实例数随分片规模增长。单线程足够，前提是挂在上面的任务不阻塞：探测须走 ZK background 调用，
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
