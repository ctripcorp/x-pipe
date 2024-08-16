package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;


import java.util.*;

public class ConsoleMetaCacheWithoutDB extends DefaultMetaCache {

    private ConsolePortalService consolePortalService;

    public ConsoleMetaCacheWithoutDB(ConsolePortalService consolePortalService) {
        this.consolePortalService = consolePortalService;
    }

    @Override
    void loadCache() throws Exception {

        TransactionMonitor.DEFAULT.logTransaction("MetaCache", "load", new Task() {

            @Override
            public void go() throws Exception {
                XpipeMeta xpipeMeta = consolePortalService.getXpipeAllMeta(lastUpdateTime);
                refreshMetaParts();
                refreshMeta(xpipeMeta);
            }

            @Override
            public Map getData() {
                return null;
            }
        });
    }

    public void startLoadMeta() {

        logger.info("[loadMeta][start]{}", this);
        while (true) {
            try {
                loadCache();
            } catch (Exception e) {
                try {
                    // 如果抛异常，特别是刚启动时候，不能大量请求
                    Thread.sleep(500);
                } catch (InterruptedException ex) {
                    logger.error("[loadMeta][interrupted]", ex);
                }
                logger.error("[loadMeta]", e);
            }
        }
    }
}
