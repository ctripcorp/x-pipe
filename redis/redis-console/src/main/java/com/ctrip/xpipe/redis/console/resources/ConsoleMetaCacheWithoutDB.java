package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;


import java.util.*;
import java.util.concurrent.TimeUnit;

public class ConsoleMetaCacheWithoutDB extends DefaultMetaCache {

    private ConsolePortalService consolePortalService;

    private ConsoleConfig config;

    public ConsoleMetaCacheWithoutDB(ConsolePortalService consolePortalService, ConsoleConfig config) {
        this.consolePortalService = consolePortalService;
        this.config = config;
    }

    @Override
    void loadCache() throws Exception {

        TransactionMonitor.DEFAULT.logTransaction("MetaCacheApi", "load", new Task() {

            @Override
            public void go() {
                try {
                    XpipeMeta xpipeMeta = consolePortalService.getXpipeAllMeta(getVersion());
                    checkMeta(xpipeMeta, config.maxRemovedDcsCnt(), config.maxRemovedClustersPercent());
                    refreshMetaParts();
                    refreshMeta(xpipeMeta);
                } catch (Throwable th) {
                    logger.error("[MetaCacheApi][load]", th);
                }

            }

            @Override
            public Map getData() {
                return null;
            }
        });
    }

    public void startLoadMeta() {

        logger.info("[loadMeta][start]{}", this);

        long refreshIntervalMilli = config.getCacheRefreshInterval();

        future = scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                if(!taskTrigger.get())
                    return;
                loadCache();
            }

        }, 1000, refreshIntervalMilli, TimeUnit.MILLISECONDS);
    }
}
