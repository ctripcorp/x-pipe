package com.ctrip.xpipe.redis.console.health.sentinel.monitor;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 * <p>
 * Feb 23, 2018
 */
@Component
public class DefaultSentinelMonitorsCheck extends AbstractSentinelMonitorsCheck {

    @Autowired
    private AlertManager alertManager;

    @Override
    public void checkSentinel(SentinelMeta sentinelMeta, HostPort sentinelHostPort) {
        Sentinel sentinel = new Sentinel(sentinelHostPort.toString(), sentinelHostPort.getHost(), sentinelHostPort.getPort());
        String infoSentinel = sentinelManager.infoSentinel(sentinel);

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
}
