package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public abstract class AbstractClusterShardPeriodicTask extends AbstractStartStoppable implements Releasable, Startable, Stoppable {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected DcMetaCache dcMetaCache;

    protected CurrentMetaManager currentMetaManager;

    protected ScheduledExecutorService scheduled;

    private ScheduledFuture<?> future;

    protected String clusterId, shardId;

    protected int checkIntervalSeconds;

    public AbstractClusterShardPeriodicTask(String clusterId, String shardId, DcMetaCache dcMetaCache,
                                            CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled, int checkIntervalSeconds) {

        this.dcMetaCache = dcMetaCache;
        this.currentMetaManager = currentMetaManager;
        this.scheduled = scheduled;
        this.clusterId = clusterId;
        this.shardId = shardId;
        this.checkIntervalSeconds = checkIntervalSeconds;
    }

    @Override
    protected void doStart() throws Exception {

        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                work();
            }

        }, 0, checkIntervalSeconds, TimeUnit.SECONDS);
    }

    @Override
    protected void doStop() throws Exception {

        if (future != null) {
            logger.info("[doStop]");
            future.cancel(true);
        }

    }

    protected abstract void work();

    @Override
    public void release() throws Exception {
        stop();
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s", getClass().getSimpleName(), clusterId, shardId);
    }

}
