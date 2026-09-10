package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.SLAVE_STATE;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.keeper.SLAVE_STATE.*;

/**
 * 跨 region 非 keeper slave 的串行全量协调器。
 *
 * 以「租约」为唯一事实来源，不再依赖 slave 异步变化的 {@link SLAVE_STATE} 来判断谁在占名额：
 * <pre>
 *   放行(releaser.accept)     → 授予租约（key=ip:port，跨重连稳定）
 *   全量完成 + grace 过期     → 释放租约
 *   断链                      → 直接删除租约（重连后重新排队，保证顺序）
 * </pre>
 *
 * 严格按 ip:port 字典序放行；grace 起点由 slave 自身维护（{@link RedisSlave#getGraceStart()}，
 * 在 ack putOnline 时赋值），这里实时计算「ONLINE 且距 graceStart 不满 graceMillis」仍占名额。
 *
 * maxLoadingSlavesCnt / graceMillis / settleMillis 均通过 Supplier 每次 tick 动态读取，配置变更即时生效。
 * 结算窗口：第一次看到等待批次时起 settleMillis 的窗口，窗口内连上的 slave 成一批，满窗口后一起按 ip:port 排序串行放行。
 */
public class CrossRegionFsyncCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(CrossRegionFsyncCoordinator.class);

    private long settleDeadline = -1;                                // 结算窗口 deadline（-1 表示无正在结算的批次）

    /** 租约表：key=ip:port（跨重连稳定），断链即删除 */
    private final Set<String> lease = new HashSet<>();

    /** 放行顺序（ip:port），仅用于测试验证按序全量 */
    private final List<String> admitOrder = new ArrayList<>();

    private final IntSupplier  maxLoadingSlavesCntSupplier;
    private final LongSupplier graceMillisSupplier;
    private final LongSupplier settleMillisSupplier;
    private final LongSupplier clock;

    public CrossRegionFsyncCoordinator(IntSupplier maxLoadingSlavesCntSupplier,
                                       LongSupplier graceMillisSupplier,
                                       LongSupplier settleMillisSupplier) {
        this(maxLoadingSlavesCntSupplier, graceMillisSupplier, settleMillisSupplier, System::currentTimeMillis);
    }

    @VisibleForTesting
    public CrossRegionFsyncCoordinator(IntSupplier maxLoadingSlavesCntSupplier,
                                       LongSupplier graceMillisSupplier,
                                       LongSupplier settleMillisSupplier,
                                       LongSupplier clock) {
        this.maxLoadingSlavesCntSupplier = maxLoadingSlavesCntSupplier;
        this.graceMillisSupplier = graceMillisSupplier;
        this.settleMillisSupplier = settleMillisSupplier;
        this.clock = clock;
    }

    /** 降级/切换时重置协调器：清空租约与结算窗口，避免残留租约导致续跑绕过串行 */
    public synchronized void reset() {
        lease.clear();
        settleDeadline = -1;
        admitOrder.clear();
    }

    /**
     * fullSyncToSlave 入口。
     * @return true=继续全量，false=挂起（调用方执行 waitForSeqFsync）
     */
    public synchronized boolean onFullSyncRequest(RedisSlave slave) {
        if (maxLoadingSlavesCntSupplier.getAsInt() < 0) return true;  // 功能禁用：直接放行

        String key = key(slave);

        if (lease.contains(key)) {
            // 只有「连接中、WAIT_RDB_DUMPING」的 dumper resume 才续跑（同一个全量的继续）
            if (slave.getSlaveState() == REDIS_REPL_WAIT_RDB_DUMPING) {
                return true;
            }
            // 断链重连 / ONLINE 重连（新连接 state=null）→ 删掉旧租约，重新排队
            lease.remove(key);
        }

        if (slave.isColdStart()) return true;                        // 冷启动直接放行
        return false;                                                // 其余一律排队
    }

    /**
     * 周期调度：以租约结算并放行等待中的 slave。
     * @param slaves 当前活连接集合（redisClients 中的 RedisSlave）
     * @param releaser 放行回调（continueFsyncToSlave → doFullSync，绕过 gate）
     */
    public synchronized void tick(Set<RedisSlave> slaves, Consumer<RedisSlave> releaser) {
        long now = clock.getAsLong();
        int  maxLoadingSlavesCnt = maxLoadingSlavesCntSupplier.getAsInt();
        long graceMillis = graceMillisSupplier.getAsLong();
        long settleMillis = settleMillisSupplier.getAsLong();

        cleanLeases(slaves, now, graceMillis);

        // 1. 当前等待集合（WAIT_SEQ_FSYNC 且未断链且未获租约）
        Set<RedisSlave> waiting = slaves.stream()
                .filter(s -> !s.isKeeper() && s.isOpen()
                        && s.getSlaveState() == REDIS_REPL_WAIT_SEQ_FSYNC
                        && !lease.contains(key(s)))
                .collect(Collectors.toSet());

        if (waiting.isEmpty()) {
            settleDeadline = -1;
            return;
        }

        // 2. 名额占用（租约数）
        if (lease.size() >= maxLoadingSlavesCnt) {
            return;                                                  // 名额满，不放行（结算窗口保持）
        }

        // 3. 结算窗口：第一次看到等待批次时起窗口，窗口内继续收集
        if (settleDeadline == -1) {
            settleDeadline = now + settleMillis;
            return;
        }
        if (now < settleDeadline) {
            return;                                                  // 结算中，继续收集
        }

        // 4. 已满结算窗口：严格按 ip:port 字典序串行放行
        int remaining = maxLoadingSlavesCnt - lease.size();
        for (RedisSlave s : orderedWaiting(waiting)) {
            if (remaining <= 0) break;
            logger.info("[tick][admit]{}", s);
            releaser.accept(s);
            lease.add(key(s));                     // 授予租约
            remaining--;
        }
        settleDeadline = -1;
    }

    private void cleanLeases(Set<RedisSlave> slaves, long now, long graceMillis) {
        Map<String, RedisSlave> live = new HashMap<>();
        for (RedisSlave s : slaves) {
            if (s.isKeeper()) continue;
            live.put(key(s), s);
        }

        Iterator<String> it = lease.iterator();
        while (it.hasNext()) {
            String k = it.next();
            RedisSlave s = live.get(k);
            if (s != null && s.isOpen()) {
                // 在线：若已进入增量且 grace 过期 → 释放租约
                if (s.getSlaveState() == REDIS_REPL_ONLINE && now - s.getGraceStart() >= graceMillis) {
                    admitOrder.add(key(s));                // 记录同步完成顺序
                    it.remove();
                }
            } else {
                // 断链：直接删除租约，重连后重新排队（保证 ip:port 顺序）
                it.remove();
            }
        }
    }

    private List<RedisSlave> orderedWaiting(Set<RedisSlave> waiting) {
        return waiting.stream()
                .sorted(Comparator.comparing(this::key))             // ip:port 字典序
                .collect(Collectors.toList());
    }

    private String key(RedisSlave s) {
        int port = s.getSlaveListeningPort();
        return port > 0 ? s.ip() + ":" + port : s.ip();
    }

    @VisibleForTesting
    public synchronized int occupiedCount4Test(Set<RedisSlave> slaves) {
        cleanLeases(slaves, clock.getAsLong(), graceMillisSupplier.getAsLong());
        return lease.size();
    }

    @VisibleForTesting
    public synchronized int waitingCount4Test(Set<RedisSlave> slaves) {
        return (int) slaves.stream()
                .filter(s -> !s.isKeeper() && s.isOpen()
                        && s.getSlaveState() == REDIS_REPL_WAIT_SEQ_FSYNC
                        && !lease.contains(key(s)))
                .count();
    }

    @VisibleForTesting
    public synchronized List<String> admitOrder4Test() {
        return new ArrayList<>(admitOrder);
    }
}
