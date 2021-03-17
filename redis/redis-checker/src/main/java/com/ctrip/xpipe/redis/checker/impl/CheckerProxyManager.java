package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.api.cluster.LeaderAware;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.ProxyManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author lishanglin
 * date 2021/3/11
 */
public class CheckerProxyManager extends AbstractService implements ProxyManager, LeaderAware {

    private Map<DcClusterShard, ProxyTunnelInfo> proxyTunnelInfos;

    private CheckerConsoleService checkerConsoleService;

    private CheckerConfig config;

    private DynamicDelayPeriodTask proxyTunnelsRefreshTask;

    private ScheduledExecutorService scheduled;

    private volatile boolean isLeader = false;

    private Logger logger = LoggerFactory.getLogger(CheckerProxyManager.class);

    @Autowired
    public CheckerProxyManager(CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService) {
        this.checkerConsoleService = checkerConsoleService;
        this.config = checkerConfig;
        this.proxyTunnelInfos = new HashMap<>();
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("ProxyTunnelsRefresher"));
        this.proxyTunnelsRefreshTask = new DynamicDelayPeriodTask("ProxyTunnelsRefresher", this::refreshProxyTunnels, config::getCheckerMetaRefreshIntervalMilli, scheduled);
    }

    private void refreshProxyTunnels() {
        try {
            logger.debug("[refreshProxyTunnels] start");
            List<ProxyTunnelInfo> tunnelInfos = checkerConsoleService.getProxyTunnelInfos(config.getConsoleAddress());
            Map<DcClusterShard, ProxyTunnelInfo> newInfos = new HashMap<>();
            tunnelInfos.forEach(proxyTunnelInfo -> newInfos.put(new DcClusterShard(proxyTunnelInfo.getBackupDcId(),
                    proxyTunnelInfo.getClusterId(), proxyTunnelInfo.getShardId()), proxyTunnelInfo));

            this.proxyTunnelInfos = newInfos;
        } catch (Throwable th) {
            logger.info("[refreshProxyTunnels] fail", th);
        }
    }

    @Override
    public List<ProxyTunnelInfo> getAllProxyTunnels() {
        return new ArrayList<>(proxyTunnelInfos.values());
    }

    @Override
    public ProxyTunnelInfo getProxyTunnelInfo(String backupDcId, String clusterId, String shardId) {
        if (!isLeader) return null;
        return proxyTunnelInfos.get(new DcClusterShard(backupDcId, clusterId, shardId));
    }

    @Override
    public void closeProxyTunnel(ProxyTunnelInfo proxyTunnelInfo) {
        List<HostPort> backends = proxyTunnelInfo.getBackends();

        for(HostPort backend : backends) {
            try {
                logger.info("[closeProxyTunnel][{}] close", backend);
                restTemplate.delete(String.format("http://%s:8080/api/tunnel/local/port/%d", backend.getHost(), backend.getPort()));
            } catch (Exception e) {
                logger.error("[closeProxyTunnel][{}] fail", backend, e);
            }
        }
    }

    @Override
    public void isleader() {
        try {
            logger.info("[isleader] become leader");
            isLeader = true;
            proxyTunnelsRefreshTask.start();
        } catch (Throwable th) {
            logger.info("[isleader] refresh task start fail", th);
        }
    }

    @Override
    public void notLeader() {
        try {
            logger.info("[isleader] loss leader");
            isLeader = false;
            proxyTunnelsRefreshTask.stop();
        } catch (Throwable th) {
            logger.info("[notLeader] refresh task stop fail", th);
        }
    }
}
