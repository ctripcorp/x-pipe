package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMasterReplication;
import com.ctrip.xpipe.redis.keeper.SERVER_TYPE;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.impl.RdbonlyRedisMasterReplication;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateActive;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;
import com.ctrip.xpipe.redis.keeper.monitor.MasterStats;
import com.ctrip.xpipe.redis.keeper.monitor.PsyncFailReason;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author chen.zhu
 * <p>
 * Mar 02, 2020
 */
public class LeakyBucketBasedMasterReplicationListener implements RedisMasterReplication.RedisMasterReplicationObserver {

    private static final Logger logger = LoggerFactory.getLogger(LeakyBucketBasedMasterReplicationListener.class);

    private final int MAX_PARTIAL_SYNC_DELAY_TIME_SECONDS = 60;

    private RedisMasterReplication redisMasterReplication;

    private RedisKeeperServer redisKeeperServer;

    private KeeperResourceManager resourceManager;

    protected AtomicBoolean holdToken = new AtomicBoolean(false);

    private ScheduledExecutorService scheduled;

    protected ScheduledFuture<?> releaseTokenFuture;

    private AtomicLong psyncSendUnixTime;

    public LeakyBucketBasedMasterReplicationListener(RedisMasterReplication redisMasterReplication,
                                                     RedisKeeperServer redisKeeperServer,
                                                     KeeperResourceManager resourceManager,
                                                     ScheduledExecutorService scheduled) {
        this.redisMasterReplication = redisMasterReplication;
        this.redisKeeperServer = redisKeeperServer;
        this.resourceManager = resourceManager;
        this.scheduled = scheduled;
    }

    @Override
    public void onMasterConnected() {
        initAndClear();
    }

    @Override
    public boolean canSendPsync() {
        if (redisMasterReplication.redisMaster().isKeeper()
                && (redisKeeperServer.getRedisKeeperServerState() instanceof RedisKeeperServerStateActive)) {

            KeeperStats keeperStats = redisKeeperServer.getKeeperMonitor().getKeeperStats();
            MasterStats masterStats = redisKeeperServer.getKeeperMonitor().getMasterStats();

            if(masterStats.lastMasterType() != SERVER_TYPE.REDIS
                    && !(redisMasterReplication instanceof RdbonlyRedisMasterReplication)
                    && replDownShortly()){
                logger.info("[canSendPsync] psync pass {} {}", masterStats.lastMasterType(), redisMasterReplication);
                return true;
            }

            //care 2 cases:
            //1. DR Switch
            //2. Down for a long time
            if(resourceManager.getLeakyBucket().tryAcquire()) {
                logger.warn("[canSendPsync]leak acquire succeed, psync pass {}", redisMasterReplication);
                holdToken.set(true);
                recordPsyncSendTime();
            } else {
                logger.warn("[canSendPsync]leak acquire failed, psync blocked {}", redisMasterReplication);
                keeperStats.setLastPsyncFailReason(PsyncFailReason.TOKEN_LACK);
                keeperStats.increasePsyncSendFail();
                return false;
            }
        }
        return true;
    }

    private boolean replDownShortly() {
        long lastReplDownTime = redisKeeperServer.getKeeperMonitor().getReplicationStoreStats().getLastReplDownTime();
        long replDownSafeIntervalMilli = redisKeeperServer.getKeeperConfig().getReplDownSafeIntervalMilli();

        long currentTime = System.currentTimeMillis();
        boolean result = currentTime - lastReplDownTime < replDownSafeIntervalMilli;
        logger.info("[replDownShortly]({} - {} = {}) < {} : {}, {}",
                DateTimeUtils.timeAsString(currentTime),
                DateTimeUtils.timeAsString(lastReplDownTime),
                currentTime - lastReplDownTime, result, replDownSafeIntervalMilli,redisMasterReplication);
        return result;
    }

    @Override
    public void onMasterDisconnected() {
        releaseToken("onMasterDisconnected");
        cancelReleaseFuture();
    }

    @Override
    public void endWriteRdb() {
        releaseToken("endWriteRdb");
    }

    @Override
    public void onContinue(String requestReplId, String responseReplId) {
        doOnContinue();
    }

    @Override
    public void onKeeperContinue(String replId, long beginOffset) {
        doOnContinue();
    }

    private void doOnContinue() {
        if(holdToken.get()){
            tryDelayReleaseToken();
        }
        setPsyncSucceed();
    }

    @Override
    public void onFullSync(long masterRdbOffset) {
        setPsyncSucceed();
    }

    @Override
    public void readAuxEnd(RdbStore rdbStore, Map<String, String> auxMap) {

    }

    @Override
    public void reFullSync() {
    }

    @Override
    public void onDumpFinished() {

    }

    @Override
    public void onDumpFail(Throwable th) {
        releaseToken("onDumpFail");
        cancelReleaseFuture();

        setPsyncFailed(th);
    }

    @Override
    public void beginWriteRdb(EofType eofType, String replId, long masterRdbOffset) throws IOException {

    }

    private void initAndClear() {
        releaseToken("initAndClear");
        cancelReleaseFuture();
    }

    private void cancelReleaseFuture() {

        if (releaseTokenFuture != null && !releaseTokenFuture.isDone()) {
            releaseTokenFuture.cancel(true);
            releaseTokenFuture = null;
        }
    }

    @VisibleForTesting
    protected void releaseToken(String from) {

        if(holdToken.compareAndSet(true, false)){
            logger.info("[releaseToken]succeed [{}], {}", from, redisMasterReplication);
            resourceManager.getLeakyBucket().release();
        }
    }

    private void recordPsyncSendTime() {
        if (psyncSendUnixTime == null) {
            psyncSendUnixTime = new AtomicLong(System.currentTimeMillis());
        } else {
            psyncSendUnixTime.set(System.currentTimeMillis());
        }
    }

    private void tryDelayReleaseToken() {

        KeeperConfig keeperConfig = redisKeeperServer.getKeeperConfig();
        long replicationTrafficHighWaterMark = keeperConfig.getReplicationTrafficHighWaterMark();
        long totalBucketSize = resourceManager.getLeakyBucket().getTotalSize();

        long commandBPS = redisKeeperServer.getKeeperMonitor().getMasterStats().getCommandBPS();
        long lastReplDownTime = redisKeeperServer.getKeeperMonitor().getReplicationStoreStats().getLastReplDownTime();

        long downTimeSeconds = (System.currentTimeMillis() - lastReplDownTime)/1000;
        long delayTimeSeconds =  downTimeSeconds * commandBPS/replicationTrafficHighWaterMark * totalBucketSize;

        logger.info("[tryDelayReleaseToken]{}*{}/{}*{}={}, {}", downTimeSeconds, commandBPS, replicationTrafficHighWaterMark, totalBucketSize, delayTimeSeconds, redisMasterReplication);
        if(delayTimeSeconds > MAX_PARTIAL_SYNC_DELAY_TIME_SECONDS){
            delayTimeSeconds = MAX_PARTIAL_SYNC_DELAY_TIME_SECONDS;
        }
        delayReleaseToken((int) (delayTimeSeconds * 1000));
    }

    protected void delayReleaseToken(final int delayTimeMilli) {
        releaseTokenFuture = scheduled.schedule(new AbstractExceptionLogTask() {
            @Override
            public void doRun() {
                releaseToken("delayReleaseToken");
            }
        }, delayTimeMilli, TimeUnit.MILLISECONDS);
    }

    private void setPsyncFailed(Throwable th) {
        redisKeeperServer.getKeeperMonitor().getKeeperStats().setLastPsyncFailReason(PsyncFailReason.from(th));
    }

    private void setPsyncSucceed() {
        redisKeeperServer.getKeeperMonitor().getKeeperStats().setLastPsyncFailReason(null);
    }

    @Override
    public void onXFullSync(String replId, long replOff, String masterUuid, GtidSet gtidLost) {
        setPsyncSucceed();
    }

    @Override
    public void onXContinue(String replId, long replOff, String masterUuid, GtidSet gtidCont) {
        doOnContinue();
    }

    @Override
    public void onSwitchToXsync(String replId, long replOff, String masterUuid) {
        doOnContinue();
    }

    @Override
    public void onSwitchToPsync(String replId, long replOff) {
        doOnContinue();
    }

    @Override
    public void onUpdateXsync() {
    }
}
