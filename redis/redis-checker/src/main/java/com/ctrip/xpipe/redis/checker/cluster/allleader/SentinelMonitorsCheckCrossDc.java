package com.ctrip.xpipe.redis.checker.cluster.allleader;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.cluster.allleader.sentinel.SentinelMonitors;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.impl.CheckerAllMetaCache;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.catalina.Host;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Set;

@Component
public class SentinelMonitorsCheckCrossDc extends AbstractAllCheckerLeaderTask {
    protected CheckerAllMetaCache metaCache;
    
    @Autowired
    PersistenceCache persistenceCache;
    
    @Autowired
    FoundationService foundationService;
    
    @Autowired
    protected CheckerConfig config;
    
    @Autowired
    CheckerConsoleService checkerConsoleService;

    @Override
    public void isleader() {
        if(metaCache == null) {
            metaCache = new CheckerAllMetaCache(config, checkerConsoleService);
            metaCache.start();
        }
        super.isleader();
    }

    @Override
    public void notLeader() {
        if(metaCache != null) {
            metaCache.stop();
            metaCache = null; 
        }
        super.notLeader();
    }

    protected List<DcMeta> dcsToCheck() {
        List<DcMeta> result = Lists.newArrayList();
        result.add(metaCache.getXpipeMeta().getDcs().get(foundationService.getDataCenter()));
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
    
    @Autowired
    protected SentinelManager sentinelManager;
    
    @Autowired
    private AlertManager alertManager;
    
    public void checkSentinel(SentinelMeta sentinelMeta, HostPort sentinelHostPort) {
        Sentinel sentinel = new Sentinel(sentinelHostPort.toString(), sentinelHostPort.getHost(), sentinelHostPort.getPort());
        String infoSentinel = sentinelManager.infoSentinel(sentinel);
        
        if(infoSentinel == null) {
            logger.warn("[checkSentinel] info sentinel empty: {}", sentinel);
            return;
        }
        
        if(!persistenceCache.isSentinelAutoProcess()) {
            return;
        }
        
        SentinelMonitors sentinelMonitors = SentinelMonitors.parseFromString(infoSentinel);
        
        for(String monitorName: sentinelMonitors.getMonitors()) {
            if(metaCache.findClusterShardBySentinelMonitor(monitorName) == null) {
                sentinelManager.removeSentinelMonitor(sentinel, monitorName);
                String message = String.format("Sentinel cmonitor: %s not exist in Xpipe", monitorName);
                alertManager.alert(null, null, null, ALERT_TYPE.SENTINEL_MONITOR_INCONSIS, message);
            }
        }
    }

    @Override
    public Long getDelay() {
        return 1000L;
    }

    @Override
    public boolean shouldCheck() {
        return persistenceCache.isSentinelAutoProcess();
    }
    
    @VisibleForTesting
    public void setMetaCache(CheckerAllMetaCache metaCache) {
        this.metaCache= metaCache;
    }
    
}
