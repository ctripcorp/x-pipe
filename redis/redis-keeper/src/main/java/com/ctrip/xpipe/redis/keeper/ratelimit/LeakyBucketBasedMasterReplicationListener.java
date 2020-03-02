package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMasterReplication;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateActive;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.IpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chen.zhu
 * <p>
 * Mar 02, 2020
 */
public class LeakyBucketBasedMasterReplicationListener implements RedisMasterReplication.RedisMasterReplicationObserver {

    private static final Logger logger = LoggerFactory.getLogger(LeakyBucketBasedMasterReplicationListener.class);

    public final static String KEY_CHECK_PARTIAL_SYNC_INTERVAL = "keeper.check.partial.traffic.milli";
    private int checkPartialSyncInterval = Integer.parseInt(System.getProperty(KEY_CHECK_PARTIAL_SYNC_INTERVAL, "1000"));

    private RedisMasterReplication redisMasterReplication;

    private RedisKeeperServer redisKeeperServer;

    private KeeperResourceManager resourceManager;

    protected AtomicBoolean holdToken = new AtomicBoolean(false);

    private ScheduledExecutorService scheduled;

    protected ScheduledFuture<?> releaseTokenFuture;

    private volatile long latestTps;

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
            if(resourceManager.getLeakyBucket().tryAcquire()) {
                holdToken.set(true);
            } else {
                logger.warn("[canSendPsync]psync wont send as no token is available [port:{}]", redisKeeperServer.getListeningPort());
                return false;
            }
        }
        return true;
    }

    @Override
    public void onMasterDisconnected() {
        if (holdToken.compareAndSet(true, false)) {
            resourceManager.getLeakyBucket().release();
        }
        if (releaseTokenFuture != null && !releaseTokenFuture.isDone()) {
            releaseTokenFuture.cancel(true);
            releaseTokenFuture = null;
        }
    }

    @Override
    public void endWriteRdb() {
        if (holdToken.compareAndSet(true, false)) {
            resourceManager.getLeakyBucket().release();
        }
    }

    @Override
    public void onContinue(String requestReplId, String responseReplId) {
        tryDelayReleaseToken();
    }

    @Override
    public void onDumpFinished() {

    }

    @Override
    public void onDumpFail() {
        if (holdToken.compareAndSet(true, false)) {
            resourceManager.getLeakyBucket().release();
        }
    }

    private void initAndClear() {
        holdToken.set(false);
        if (releaseTokenFuture != null) {
            releaseTokenFuture.cancel(true);
            releaseTokenFuture = null;
        }
    }

    private void tryDelayReleaseToken() {
        if (holdToken.get()) {
            KeeperConfig keeperConfig = redisKeeperServer.getKeeperConfig();
            // num(files) * file-size / cross-dc-replication-rate
            long afterMilli = 1000 * keeperConfig.getReplicationStoreCommandFileNumToKeep()
                    * keeperConfig.getReplicationStoreCommandFileSize() / keeperConfig.getReplicationTrafficHighWaterMark();
            long deadline = afterMilli + System.currentTimeMillis();
            logger.info("[tryDelayReleaseToken] release at most {}", DateTimeUtils.timeAsString(deadline));
            checkIfNeedReleaseToken(deadline);
        }
    }

    //either obvious speed shrink down, or under pre-defined threshold (low water mark)
    //we consider TCP traffic is safe enough to release the token
    private void checkIfNeedReleaseToken(final long deadline) {
        logger.debug("[checkIfNeedReleaseToken]");
        releaseTokenFuture = scheduled.schedule(new Runnable() {
            @Override
            public void run() {
                if (holdToken.get()) {
                    if(isTokenReadyToRelease(deadline)) {
                        holdToken.set(false);
                        resourceManager.getLeakyBucket().release();
                    } else {
                        checkIfNeedReleaseToken(deadline);
                    }
                }
            }
        }, checkPartialSyncInterval, TimeUnit.MILLISECONDS);
    }

    private boolean isTokenReadyToRelease(final long deadline) {
        if (System.currentTimeMillis() >= deadline) {
            return true;
        }
        long tps = redisKeeperServer.getKeeperMonitor().getKeeperStats().getInputInstantaneousBPS();
        if (tps < latestTps/2 || tps <= redisKeeperServer.getKeeperConfig().getReplicationTrafficLowWaterMark()) {
            return true;
        }
        latestTps = tps;
        return false;
    }

    @Override
    public void onFullSync() {

    }

    @Override
    public void reFullSync() {

    }

    @Override
    public void beginWriteRdb(EofType eofType, long masterRdbOffset) throws IOException {

    }
}
