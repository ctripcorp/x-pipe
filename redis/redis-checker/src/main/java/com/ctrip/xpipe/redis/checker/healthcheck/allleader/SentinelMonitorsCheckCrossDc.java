package com.ctrip.xpipe.redis.checker.healthcheck.allleader;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.allleader.sentinel.SentinelMonitors;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class SentinelMonitorsCheckCrossDc extends AbstractAllCheckerLeaderTask {
    
    protected MetaCache metaCache;
    
    private PersistenceCache persistenceCache;
    
    private CheckerConfig config;
    
    private String currentDcId;
    
    private SentinelManager sentinelManager;
    
    private AlertManager alertManager;

    public SentinelMonitorsCheckCrossDc(
            MetaCache metaCache,
            PersistenceCache persistenceCache,
            CheckerConfig config,
            String currentDcId,
            SentinelManager sentinelManager,
            AlertManager alertManager
            ) {
        this.persistenceCache = persistenceCache;
        this.config = config;
        this.currentDcId = currentDcId;
        this.sentinelManager = sentinelManager;
        this.alertManager = alertManager;
        this.metaCache = metaCache;
    }
    


    @Override
    public void isleader() {
        super.isleader();
        logger.info("[SentinelMonitorsCheckCrossDc] start");
    }

    @Override
    public void notLeader() {
        super.notLeader();
        logger.info("[SentinelMonitorsCheckCrossDc] stop");
    }

    protected List<DcMeta> dcsToCheck() {
        List<DcMeta> result = Lists.newArrayList();
        result.add(metaCache.getXpipeMeta().getDcs().get(currentDcId));
        Set<String> ignoreDcNames = config.getIgnoredHealthCheckDc();
        List<DcMeta> toRemove = Lists.newArrayList();
        for(DcMeta dcMeta: result) {
            if(ignoreDcNames.contains(dcMeta.getId()) || ignoreDcNames.contains(dcMeta.getId().toLowerCase())) {
                toRemove.add(dcMeta);
            }
        }
        result.removeAll(toRemove);
        return result;
    }
    
    @Override
    public void doTask() {
        logger.debug("[doCheck] check sentinel monitors");
        if(metaCache == null || metaCache.getXpipeMeta() == null) return;
        Collection<DcMeta> dcMetas = dcsToCheck();
        for(DcMeta dcMeta: dcMetas) {
            Collection<SentinelMeta> sentinelMetas = dcMeta.getSentinels().values();
            for(SentinelMeta sentinelMeta: sentinelMetas) {
                List<HostPort> sentinels = IpUtils.parseAsHostPorts(sentinelMeta.getAddress());
                for(HostPort hostPort: sentinels) {
                    checkSentinel(sentinelMeta, hostPort);
                }
            }
        }
    }
    
    
    public void checkSentinel(SentinelMeta sentinelMeta, HostPort sentinelHostPort) {
        Sentinel sentinel = new Sentinel(sentinelHostPort.toString(), sentinelHostPort.getHost(), sentinelHostPort.getPort());
        String infoSentinel = infoSentinel(sentinel);
        
        if(infoSentinel == null) {
            logger.warn("[checkSentinel] info sentinel empty: {}", sentinel);
            return;
        }
        
        if(!persistenceCache.isSentinelAutoProcess()) {
            return;
        }
        
        SentinelMonitors sentinelMonitors = SentinelMonitors.parseFromString(infoSentinel);
        
        for(String monitorName: sentinelMonitors.getMonitors()) {
            SentinelUtil.SentinelInfo sentinelInfo = SentinelUtil.SentinelInfo.fromMonitorName(monitorName);
            ClusterType clusterType = ClusterType.lookup(sentinelMeta.getClusterType());
            if (!config.supportSentinelHealthCheck(clusterType, sentinelInfo.getClusterName()))
                continue;
            if (metaCache.findClusterShardBySentinelMonitor(monitorName) == null) {
                try {
                    sentinelManager.removeSentinelMonitor(sentinel, monitorName).execute().get();
                } catch (Exception e) {
                    logger.error("[checkSentinel] removeSentinelMonitor failed: {},{}", sentinel, monitorName, e);
                }
                CatEventMonitor.DEFAULT.logEvent("Sentinel.Monitors.Check.Remove", monitorName);
                String message = String.format("Sentinel monitor: %s not exist in Xpipe", monitorName);
                alertManager.alert(null, null, null, ALERT_TYPE.SENTINEL_MONITOR_INCONSIS, message);
            }
        }
    }

    String infoSentinel(Sentinel sentinel) {
        String infoSentinel = null;
        try {
            infoSentinel = sentinelManager.infoSentinel(sentinel).execute().get(2050, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("[checkSentinel] infoSentinel failed: {}", sentinel, e);
        }
        return infoSentinel;
    }

    @Override
    public int getDelay() {
        return this.config.getRedisConfCheckIntervalMilli();
    }

    @Override
    public boolean shouldCheck() {
        return persistenceCache.isSentinelAutoProcess();
    }
    
    @VisibleForTesting
    public void setMetaCache(MetaCache metaCache) {
        this.metaCache= metaCache;
    }

    @Override
    public List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.SENTINEL_MONITOR_INCONSIS);
    }
}
