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
 * 不额外维护 phase，直接以 slave 自身的 {@link SLAVE_STATE} 为准：
 * <pre>
 *   REDIS_REPL_WAIT_SEQ_FSYNC            → 等待（不占名额，按 IP 排序）
 *   REDIS_REPL_WAIT_RDB_DUMPING /
 *   REDIS_REPL_SEND_BULK                 → loading（占名额，正在全量）
 *   REDIS_REPL_ONLINE                    → grace（占名额，满 5s 释放）
 * </pre>
 *
 * 严格按 IP 字典序放行；grace 起点由 slave 自身维护（{@link RedisSlave#getGraceStart()}，
 * 在 ack putOnline 时赋值），这里实时计算「ONLINE 且距 graceStart 不满 graceMillis」即占名额。
 *
 * maxLoadingSlavesCnt / graceMillis / settleMillis 均通过 Supplier 每次 tick 动态读取，配置变更即时生效。
 * 结算窗口：第一次看到等待批次时起 settleMillis 的窗口，窗口内连上的 slave 成一批，满窗口后一起按 IP 排序串行放行。
 */
public class CrossRegionFsyncCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(CrossRegionFsyncCoordinator.class);

    private long settleDeadline = -1;                                // 结算窗口 deadline（-1 表示无正在结算的批次）

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

    /**
     * fullSyncToSlave 入口。
     * @return true=继续全量，false=挂起（调用方执行 waitForSeqFsync）
     */
    public synchronized boolean onFullSyncRequest(RedisSlave slave) {
        if (!slave.isOpen()) return false;
        if (maxLoadingSlavesCntSupplier.getAsInt() < 0) return true;  // 功能禁用：直接放行

        SLAVE_STATE state = slave.getSlaveState();
        if (state == REDIS_REPL_WAIT_RDB_DUMPING) {
            return true;                                             // 已放行续跑（dumper resume/retry）
        }
        if (slave.isColdStart()) return true;                        // 冷启动直接放行
        return false;                                                // 新请求/等待/online → defer
    }

    /**
     * 周期调度：以 slaves 的实时状态结算并放行等待中的 slave。
     * @param slaves 当前活连接集合（redisClients 中的 RedisSlave）
     * @param releaser 放行回调（continueFsyncToSlave → doFullSync，绕过 gate）
     */
    public synchronized void tick(Set<RedisSlave> slaves, Consumer<RedisSlave> releaser) {
        long now = clock.getAsLong();
        int  maxLoadingSlavesCnt = maxLoadingSlavesCntSupplier.getAsInt();
        long graceMillis = graceMillisSupplier.getAsLong();
        long settleMillis = settleMillisSupplier.getAsLong();

        // 1. 当前等待集合（WAIT_SEQ_FSYNC 且未断链）
        Set<RedisSlave> waiting = slaves.stream()
                .filter(s -> !s.isKeeper() && s.isOpen() && s.getSlaveState() == REDIS_REPL_WAIT_SEQ_FSYNC)
                .collect(Collectors.toSet());

        if (waiting.isEmpty()) {
            settleDeadline = -1;
            return;
        }

        // 2. 名额占用（loading + grace，grace 实时计算）
        int occupied = occupiedCount(slaves, now, graceMillis);
        if (occupied >= maxLoadingSlavesCnt) {
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

        // 4. 已满结算窗口：严格按 IP 字典序串行放行
        int remaining = maxLoadingSlavesCnt - occupied;
        for (RedisSlave s : orderedWaiting(waiting)) {
            if (remaining <= 0) break;
            logger.info("[tick][admit]{}", s);
            releaser.accept(s);
            remaining--;                                             // 本地计数，避免异步滞后导致同 tick 超发
        }
        settleDeadline = -1;
    }

    private List<RedisSlave> orderedWaiting(Set<RedisSlave> waiting) {
        return waiting.stream()
                .sorted(Comparator.comparing(this::key))             // IP 字典序
                .collect(Collectors.toList());
    }

    private int occupiedCount(Set<RedisSlave> slaves, long now, long graceMillis) {
        int occupied = 0;
        for (RedisSlave s : slaves) {
            if (s.isKeeper() || !s.isOpen()) continue;
            SLAVE_STATE st = s.getSlaveState();
            if (st == REDIS_REPL_WAIT_RDB_DUMPING || st == REDIS_REPL_SEND_BULK) {
                occupied++;                                          // loading
            } else if (st == REDIS_REPL_ONLINE && now - s.getGraceStart() < graceMillis) {
                occupied++;                                          // grace（实时计算）
            }
        }
        return occupied;
    }

    private String key(RedisSlave s) {
        int port = s.getSlaveListeningPort();
        return port > 0 ? s.ip() + ":" + port : s.ip();
    }

    @VisibleForTesting
    public synchronized int occupiedCount4Test(Set<RedisSlave> slaves) {
        return occupiedCount(slaves, clock.getAsLong(), graceMillisSupplier.getAsLong());
    }

    @VisibleForTesting
    public synchronized int waitingCount4Test(Set<RedisSlave> slaves) {
        return (int) slaves.stream()
                .filter(s -> !s.isKeeper() && s.isOpen() && s.getSlaveState() == REDIS_REPL_WAIT_SEQ_FSYNC)
                .count();
    }
}
