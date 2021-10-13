package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class CheckerAllMetaCache extends AbstractMetaCache implements MetaCache {
    private static final Logger logger = LoggerFactory.getLogger(CheckerAllMetaCache.class);
    
    private CheckerConfig config;
    
    private CheckerConsoleService checkerConsoleService;
    
    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask metaLoadTask;
    
    private boolean pauseing = false;
    
    public CheckerAllMetaCache(CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService) {
        this.config = checkerConfig;
        this.checkerConsoleService = checkerConsoleService;
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("CheckerMetaLoader"));
        this.metaLoadTask = new DynamicDelayPeriodTask("CheckerMetaLoader", this::loadMeta, config::getCheckerMetaRefreshIntervalMilli, scheduled);
    }

    private void loadMeta() {
        try {
            synchronized (this) {
                if(pauseing) return;
                logger.debug("[loadMeta] start");
                XpipeMeta xpipeMeta = checkerConsoleService.getXpipeAllMeta(config.getConsoleAddress());
                refreshMeta(xpipeMeta);
            }
        } catch (Throwable th) {
            logger.info("[loadMeta] fail", th);
        }
    }

    
    public void start() {
        try {
            this.metaLoadTask.start();
        } catch (Throwable th) {
            logger.info("[postConstruct] start fail", th);
        }
    }

    @PreDestroy
    public void stop() {
        try {
            this.metaLoadTask.stop();
        } catch (Throwable th) {
            logger.info("[preDestroy] fail", th);
        }
    }

    //Release memory
    void cleanMetaCache() {
        this.meta = null;
        this.allKeepers = null;
        this.monitor2ClusterShard = null;
    }
    
    @Override
    public void pauseUpdate() {
        synchronized (this) {
            this.pauseing = true;
            stop();
            cleanMetaCache();
        }
    }

    @Override
    public void continueUpdate() {
        this.pauseing = false;
        start();
    }
}
