package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.Map;
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
            public void go() throws IOException, SAXException {
                try {
                    XpipeMeta xpipeMeta = consolePortalService.getXpipeAllMeta(getVersion());
                    checkMeta(xpipeMeta, config.maxRemovedDcsCnt(), config.maxRemovedClustersPercent());
                    // 本机房 ZK 由本 Console 自行监听，以本地地址覆盖上游填的 KeeperContainer IP，
                    // 再分片下发给同机房 Checker；须在 refreshMetaParts 之前，分片 XML 由此 Meta 生成
                    fillContainerKeeperIp(xpipeMeta);
                    refreshMetaParts(xpipeMeta);
                    refreshMeta(xpipeMeta);
                } catch (Throwable th) {
                    logger.error("[MetaCacheApi][load]", th);
                    throw th;
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
                if(!isLeader.get())
                    return;
                loadCache();
            }

        }, 1000, refreshIntervalMilli, TimeUnit.MILLISECONDS);
    }
}
