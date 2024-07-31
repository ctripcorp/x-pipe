package com.ctrip.xpipe.redis.keeper.ratelimit.impl;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.store.ratelimit.SyncRateLimiter;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * @author lishanglin
 * date 2024/7/26
 */
@Service
public class FixSyncRateManager extends AbstractLifecycle implements SyncRateManager, Lifecycle, TopElement {

    private static int recordCnt = Integer.parseInt(System.getProperty("keeper.sync.record.count", "60"));

    private static int recordPeriodSec = Integer.parseInt(System.getProperty("keeper.sync.record.period.sec", "1"));

    private static final RateLimiter UNLIMITED = RateLimiter.create(Double.MAX_VALUE);

    private AtomicInteger diskIOLimit;

    private AtomicLongArray psyncIORecords;

    private volatile int currentRecord = 0;

    private long avgPsyncIO;

    private RateLimiter globalFsyncRateLimiter;

    private ScheduledExecutorService scheduled;

    private ScheduledFuture<?> future;

    private static Logger logger = LoggerFactory.getLogger(FixSyncRateManager.class);

    public FixSyncRateManager() {
        this.diskIOLimit = new AtomicInteger(0);
        this.psyncIORecords = new AtomicLongArray(recordCnt);
        this.globalFsyncRateLimiter = UNLIMITED;
    }

    @Override
    public void setTotalIOLimit(int limit) {
        int pre = this.diskIOLimit.get();
        this.diskIOLimit.set(limit);
        if (pre != limit) {
            logger.info("[setTotalIOLimit]{}->{}", pre, limit);
            this.refreshRateLimiter();
        }
    }

    @Override
    public int getTotalIOLimit() {
        return this.diskIOLimit.get();
    }

    @Override
    protected void doInitialize() throws Exception {
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("Fix-Sync-Rate"));
    }

    @Override
    protected void doStart() throws Exception {
        future = scheduled.scheduleWithFixedDelay(() -> {
            logger.debug("[rotate][{}] {}", currentRecord, psyncIORecords.get(currentRecord));
            int next = this.currentRecord >= recordCnt - 1 ? 0 : this.currentRecord + 1;
            if (0 == next) {
                long totalPsyncIO = 0;
                for (int i = 0; i < psyncIORecords.length(); i++) {
                    totalPsyncIO += psyncIORecords.get(i);
                }
                this.avgPsyncIO = totalPsyncIO / psyncIORecords.length();
            }

            psyncIORecords.set(next, 0);
            this.currentRecord = next;

            if (0 == currentRecord) {
                refreshRateLimiter();
            }
        }, recordPeriodSec, recordPeriodSec, TimeUnit.SECONDS);
    }

    protected synchronized void refreshRateLimiter() {
        long totalIOLimit = diskIOLimit.get();

        logger.debug("[refreshRateLimiter] total:{}", totalIOLimit);
        if (totalIOLimit <= 0) {
            this.globalFsyncRateLimiter = UNLIMITED;
            return;
        }

        long retainIO4Psync = (long) Math.min(0.2 * totalIOLimit, Math.max(0.1 * totalIOLimit, avgPsyncIO));
        long fsyncRateLimit = (long) Math.max(0.1 * totalIOLimit, totalIOLimit - retainIO4Psync - avgPsyncIO);

        logger.debug("[refreshRateLimiter] psyncAvg:{},psyncRetain:{},fsyncLimit:{}", avgPsyncIO, retainIO4Psync, fsyncRateLimit);
        this.globalFsyncRateLimiter.setRate(fsyncRateLimit);
    }

    @Override
    protected void doStop() throws Exception {
        this.future.cancel(false);
    }

    @Override
    protected void doDispose() throws Exception {
        this.scheduled.shutdown();
    }

    @Override
    public SyncRateLimiter generateFsyncRateLimiter() {
        return new FsyncRateLimiter();
    }

    @Override
    public SyncRateLimiter generatePsyncRateLimiter() {
        return new PsyncRateLimiter();
    }

    public class FsyncRateLimiter implements SyncRateLimiter {
        @Override
        public void acquire(int syncByte) {
            if (diskIOLimit.get() <= 0) return;
            globalFsyncRateLimiter.acquire(syncByte);
        }
    }

    public class PsyncRateLimiter implements SyncRateLimiter {
        @Override
        public void acquire(int syncByte) {
            psyncIORecords.addAndGet(currentRecord, syncByte);
        }
    }

}
