package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author lishanglin
 * date 2021/3/9
 */
public class CheckerMetaCache extends AbstractMetaCache implements MetaCache {

    private static final Logger logger = LoggerFactory.getLogger(CheckerMetaCache.class);

    private CheckerConfig config;

    private CheckerConsoleService checkerConsoleService;

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask metaLoadTask;

    @Autowired
    public CheckerMetaCache(CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService) {
        this.config = checkerConfig;
        this.checkerConsoleService = checkerConsoleService;
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("CheckerMetaLoader"));
        this.metaLoadTask = new DynamicDelayPeriodTask("CheckerMetaLoader", this::loadMeta, config::getCheckerMetaRefreshIntervalMilli, scheduled);
    }

    @PostConstruct
    public void postConstruct() {
        try {
            this.metaLoadTask.start();
        } catch (Throwable th) {
            logger.info("[postConstruct] start fail", th);
        }
    }

    @PreDestroy
    public void preDestroy() {
        try {
            this.metaLoadTask.stop();
            this.scheduled.shutdownNow();
        } catch (Throwable th) {
            logger.info("[preDestroy] fail", th);
        }
    }

    private void loadMeta() {
        try {
            logger.debug("[loadMeta] start");
            XpipeMeta xpipeMeta = checkerConsoleService.getXpipeMeta(config.getConsoleAddress(), config.getClustersPartIndex());
            refreshMeta(xpipeMeta);
        } catch (Throwable th) {
            logger.info("[loadMeta] fail", th);
        }
    }

}
