package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.meta.CurrentDcAllMeta;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author yu
 * <p>
 * 2023/11/14
 */
public class CheckerCurrentDcAllMeta implements CurrentDcAllMeta {

    private static final Logger logger = LoggerFactory.getLogger(CheckerCurrentDcAllMeta.class);

    @Autowired
    private CheckerConfig config;

    @Autowired
    private CheckerConsoleService checkerConsoleService;

    private DcMeta currentDcAllMeta;

    private DynamicDelayPeriodTask metaLoadTask;

    private ScheduledExecutorService scheduled;

    private String currentDcId;

    @PostConstruct
    public void init(){
        scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("CheckerCurrentDcAllMetaLoader"));
        metaLoadTask = new DynamicDelayPeriodTask("CheckerCurrentDcAllMetaLoader", this::loadMeta,
                config::getCheckerCurrentDcAllMetaRefreshIntervalMilli, scheduled);
        this.currentDcId = FoundationService.DEFAULT.getDataCenter();

        try {
            metaLoadTask.start();
        } catch (Throwable th) {
            logger.error("start to load current dc all meta fail", th);
        }
    }


    public void preDestroy() {
        try {
            this.metaLoadTask.stop();
            this.scheduled.shutdownNow();
        } catch (Throwable th) {
            logger.error("[preDestroy] fail", th);
        }
    }

    private synchronized void loadMeta() {
        logger.debug("[loadMeta] start to load current dc all meta");
        DcMeta dcMeta = getCurrentDcAllMeta(currentDcId);
        if (dcMeta == null) {
            logger.warn("[loadMeta] get current dc meta null");
            return;
        }
        this.currentDcAllMeta = dcMeta;
    }

    private DcMeta getCurrentDcAllMeta(String dcId) {
        try {
            return checkerConsoleService.getXpipeDcAllMeta(config.getConsoleAddress(), dcId)
                    .getDcs().get(dcId);
        } catch (Throwable th) {
            logger.error("[getCurrentDcAllMeta] get dcMeta from dc {} fail", dcId, th);
        }
        return null;
    }

    @Override
    public DcMeta getCurrentDcAllMeta() {
        if (this.currentDcAllMeta == null) {
            loadMeta();
            if (this.currentDcAllMeta == null) {
                logger.error("[loadMeta] getCurrentDcAllMeta fail, generateHealthCheckInstances will be fail!");
            }
        }
        return this.currentDcAllMeta;
    }

    @VisibleForTesting
    public void setCurrentDcAllMeta(DcMeta currentDcAllMeta) {
        this.currentDcAllMeta = currentDcAllMeta;
    }
}
