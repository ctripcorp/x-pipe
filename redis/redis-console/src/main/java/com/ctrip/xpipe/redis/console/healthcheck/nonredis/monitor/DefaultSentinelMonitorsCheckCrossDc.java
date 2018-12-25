package com.ctrip.xpipe.redis.console.healthcheck.nonredis.monitor;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Feb 23, 2018
 */
@Component
public class DefaultSentinelMonitorsCheckCrossDc extends AbstractCrossDcSentinelMonitorsCheck {

    @Autowired
    private AlertManager alertManager;

    @Override
    public void checkSentinel(SentinelMeta sentinelMeta, HostPort sentinelHostPort) {
        Sentinel sentinel = new Sentinel(sentinelHostPort.toString(), sentinelHostPort.getHost(), sentinelHostPort.getPort());
        String infoSentinel = sentinelManager.infoSentinel(sentinel);

        if(infoSentinel == null) {
            logger.warn("[checkSentinel] info sentinel empty: {}", sentinel);
            return;
        }

        SentinelMonitors sentinelMonitors = SentinelMonitors.parseFromString(infoSentinel);

        // master0:name=cluster_mengshard1,status=ok,address=10.2.58.242:6399,slaves=1,sentinels=3
        for(String monitorName : sentinelMonitors.getMonitors()) {

            if(metaCache.findClusterShardBySentinelMonitor(monitorName) == null) {
                sentinelManager.removeSentinelMonitor(sentinel, monitorName);
                String message = String.format("Sentinel monitor: %s not exist in XPipe", monitorName);
                alertManager.alert(null, null, null, ALERT_TYPE.SENTINEL_MONITOR_INCONSIS, message);
            }
        }
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.SENTINEL_MONITOR_INCONSIS);
    }
}
