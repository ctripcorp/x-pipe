package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.cluster.AllCheckerLeaderAware;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class CheckerAllMetaCache extends AbstractMetaCache implements MetaCache, AllCheckerLeaderAware {
    private static final Logger logger = LoggerFactory.getLogger(CheckerAllMetaCache.class);

    @Autowired
    private CheckerConfig config;

    @Autowired
    private CheckerConsoleService checkerConsoleService;

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask metaLoadTask;
    
    private boolean pauseing = false;

    @PostConstruct
    public void init(){
        scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("CheckerMetaLoader"));
        metaLoadTask = new DynamicDelayPeriodTask("CheckerMetaLoader", this::loadMeta, config::getCheckerMetaRefreshIntervalMilli, scheduled);
    }

    private void loadMeta() {
        try {
            synchronized (this) {
                if(pauseing) return;
                logger.debug("[loadMeta] start");
                XpipeMeta xpipeMeta = checkerConsoleService.getXpipeAllMeta(config.getConsoleAddress());
                checkMeta(xpipeMeta, config.maxRemovedDcsCnt(), config.maxRemovedClustersPercent());
                refreshMeta(xpipeMeta);
            }
        } catch (Throwable th) {
            logger.error("[loadMeta] fail", th);
        }
    }

    
    private void start() {
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
    public void isleader() {
        this.pauseing = false;
        start();
    }

    @Override
    public void notLeader() {
        synchronized (this) {
            this.pauseing = true;
            stop();
            cleanMetaCache();
        }
    }

    @Override
    public Map<String, RouteMeta> chooseDefaultMetaRoutes(String clusterName, String srcDc, List<String> dstDcs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, RouteMeta> chooseClusterMetaRoutes(String clusterName, String backUpDcName, List<String> peerDcs) {
        throw new UnsupportedOperationException();
    }

}
