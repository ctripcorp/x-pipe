package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chen.zhu
 * <p>
 * Feb 25, 2020
 *
 * Token numbers would be manipulated through two stuffs:
 *  1. metaserver dynamically arrange it
 *  2. keeeper container's Apollo config would change
 *
 * So, the principle will be
 *  Apollo maintance the minimum token num we should hold
 *  MetaServer will define how many tokens we could take or close the ratelimit
 *
 * To make it simple
 *  MetaServer cannot impact the minimum token, which, if it were attempting a smaller token than Apollo says,
 *  the adjustment will not work
 *
 *  MetaServer can only offer what's more than Apollo config, but never less than
 */
public class CompositeLeakyBucket implements LeakyBucket, Startable, Stoppable {

    private static final Logger logger = LoggerFactory.getLogger(CompositeLeakyBucket.class);

    private DefaultLeakyBucket origin;

    private MetaServerKeeperService metaServerKeeperService;

    private KeeperContainerService keeperContainerService;

    private KeeperConfig keeperConfig;

    private AtomicBoolean closed = new AtomicBoolean(false);

    private ScheduledExecutorService scheduled;

    private ScheduledFuture<?> metaServerTalkProcess, checkConfigChangeProcess;

    private KeeperContainerMeta keeperContainerMeta;

    public CompositeLeakyBucket(KeeperConfig keeperConfig, MetaServerKeeperService metaServerKeeperService,
                                KeeperContainerService keeperContainerService) {
        this.metaServerKeeperService = metaServerKeeperService;
        this.keeperContainerService = keeperContainerService;
        this.keeperConfig = keeperConfig;
        this.origin = new DefaultLeakyBucket(keeperConfig.getLeakyBucketInitSize());
        String localIpAddress = Objects.requireNonNull(IpUtils.getFistNonLocalIpv4ServerAddress("10")).getHostAddress();
        this.keeperContainerMeta = new KeeperContainerMeta().setIp(localIpAddress);
    }

    @Override
    public boolean tryAcquire() {
        if(closed.get()) {
            return true;
        }
        return origin.tryAcquire();
    }

    @Override
    public void release() {
        if(closed.get()) {
            return;
        }
        origin.release();
    }

    @Override
    public void resize(int newSize) {
        if(!closed.get()) {
            origin.resize(newSize);
        }
    }

    @Override
    public int references() {
        return origin.references();
    }

    @Override
    public int getTotalSize() {
        return origin.getTotalSize();
    }

    @Override
    public void start() throws Exception {
        int threadSize = 2;
        scheduled = Executors.newScheduledThreadPool(threadSize, XpipeThreadFactory.create("leaky-bucket"));
        startTalkToMetaServer();
        checkKeeperConfigChange();
    }

    @Override
    public void stop() throws Exception {
        stopTalkToMetaServer();
        stopCheckKeeperConfig();
        if (scheduled != null) {
            scheduled.shutdownNow();
        }
    }

    private void startTalkToMetaServer() {
        int period = 10;
        metaServerTalkProcess = scheduled.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                refresh();
            }
        }, period, period, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    protected void refresh() {
        if(StringUtil.isEmpty(keeperConfig.getMetaServerAddress())) {
            logger.info("[refresh]address null, will not update token size");
            return;
        }
        List<RedisKeeperServer> keepers = keeperContainerService.list();
        int total = keepers.size(), ack = 0;
        for (RedisKeeperServer keeperServer : keepers) {
            if (keeperServer.getRedisMaster().getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED) {
                ack ++;
            }
        }
        try {
            MetaServerKeeperService.KeeperContainerTokenStatusResponse response = metaServerKeeperService.refreshKeeperContainerTokenStatus(
                    new MetaServerKeeperService.KeeperContainerTokenStatusRequest(
                            keeperContainerMeta, ack, total));
            if (response == null) {
                logger.warn("[refresh][no-response]");
                return;
            }
            closed.set(response.isClose());
            if (response.getTokenSize() > origin.getTotalSize()) {
                origin.resize(response.getTokenSize());
            }
        } catch (Exception e) {
            logger.error("[refresh]", e);
        }
    }

    private void stopTalkToMetaServer() {
        if (metaServerTalkProcess != null) {
            metaServerTalkProcess.cancel(true);
        }
    }

    private void checkKeeperConfigChange() {
        checkConfigChangeProcess = scheduled.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if(keeperConfig.getLeakyBucketInitSize() != origin.getTotalSize()) {
                    origin.resize(keeperConfig.getLeakyBucketInitSize());
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private void stopCheckKeeperConfig() {
        if (checkConfigChangeProcess != null) {
            checkConfigChangeProcess.cancel(true);
        }
    }
}
