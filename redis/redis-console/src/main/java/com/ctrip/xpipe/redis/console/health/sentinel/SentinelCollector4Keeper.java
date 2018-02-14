package com.ctrip.xpipe.redis.console.health.sentinel;

import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.redis.SentinelManager;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Feb 12, 2018
 */
@Component
public class SentinelCollector4Keeper implements SentinelCollector {

    private static Logger logger = LoggerFactory.getLogger(SentinelCollector4Keeper.class);

    @Autowired
    private SentinelManager sentinelManager;

    @Autowired
    private AlertManager alertManager;

    @Autowired
    private MetaCache metaCache;

    @Override
    public void collect(SentinelSample sentinelSample) {

        String clusterId = sentinelSample.getSamplePlan().getClusterId();
        String shardId = sentinelSample.getSamplePlan().getShardId();

        sentinelSample.getSamplePlan().getHostPort2SampleResult()
                .forEach(((hostPort, instanceSentinelResult) -> {
                    if(metaCache.inBackupDc(hostPort)) {
                        ClusterShardHostPort entry = new ClusterShardHostPort(clusterId, shardId, hostPort);
                        doCollect(entry, instanceSentinelResult);
                    }
                }));
    }

    private void doCollect(ClusterShardHostPort entry, InstanceSentinelResult result) {
        Set<SentinelHello> hellos = result.getHellos();

        for(SentinelHello hello : hellos) {
            HostPort masterAddr = hello.getMasterAddr();
            String monitorName = hello.getMonitorName();

            boolean masterGood = ObjectUtils.equals(masterAddr, metaCache.findMasterInSameShard(entry.getHostPort()));
            boolean monitorGood = StringUtil.trimEquals(monitorName,
                    metaCache.getSentinelMonitorName(entry.getClusterName(), entry.getShardName()));

            SentinelCollectorAction.getAction(masterGood, monitorGood).doAction(this, hello, entry);
        }
    }

    private Sentinel toSentinel(SentinelHello hello) {
        HostPort sentinelAddr = hello.getSentinelAddr();
        return new Sentinel(sentinelAddr.getHost(), sentinelAddr.getHost(), sentinelAddr.getPort());
    }


    public enum SentinelCollectorAction {

        MASTER_GOOD_MONITOR_GOOD {
            @Override
            public String getMessage() {
                return null;
            }

            @Override
            public void doAction(SentinelCollector4Keeper collector, SentinelHello hello, ClusterShardHostPort entry) {
            }
        },
        MASTER_GOOD_MONITOR_BAD {
            @Override
            public String getMessage() {
                return "monitor master correct, monitor name incorrect. monitor removed already";
            }

            @Override
            public void doAction(SentinelCollector4Keeper collector, SentinelHello hello, ClusterShardHostPort entry) {
                logger.error("[doAction] {}-{}-{} get from sentinel hello: {}",
                        entry.getClusterName(), entry.getShardName(), entry.getHostPort(), hello);

                collector.sentinelManager.removeSentinelMonitor(collector.toSentinel(hello), hello.getMonitorName());
                collector.alertManager.alert(entry.getClusterName(), entry.getShardName(), entry.getHostPort(),
                        ALERT_TYPE.SENTINEL_MONITOR_INCONSIS, getMessage());
            }
        },
        MASTER_BAD_MONITOR_GOOD {
            @Override
            public String getMessage() {
                return "monitor master incorrect, monitor name correct";
            }

            @Override
            public void doAction(SentinelCollector4Keeper collector, SentinelHello hello, ClusterShardHostPort entry) {
                HostPort masterAddr = collector.sentinelManager.getMasterOfMonitor(collector.toSentinel(hello),
                        hello.getMonitorName());

                // check again, get master from sentinel monitor, see if master matches
                boolean checkAgain = false;
                try {
                    checkAgain = ObjectUtils.equals(masterAddr,
                            collector.metaCache.findMaster(entry.getClusterName(), entry.getShardName()));
                } catch (Exception e) {
                    logger.error("[doAction]", e);
                }

                if(!checkAgain) {
                    collector.alertManager.alert(entry.getClusterName(), entry.getShardName(), entry.getHostPort(),
                            ALERT_TYPE.SENTINEL_MONITOR_INCONSIS, getMessage());
                } else {
                    logger.warn("[doAction] {}-{}-{} get from sentinel hello: {}",
                            entry.getClusterName(), entry.getShardName(), entry.getHostPort(), hello);
                }
            }
        },
        MASTER_BAD_MONITOR_BAD {
            @Override
            public String getMessage() {
                return "monitor master and name both incorrect for backup site redis";
            }

            @Override
            public void doAction(SentinelCollector4Keeper collector, SentinelHello hello, ClusterShardHostPort entry) {
                logger.error("[doAction] {}-{}-{} get from sentinel hello: {}",
                        entry.getClusterName(), entry.getShardName(), entry.getHostPort(), hello);
                collector.alertManager.alert(entry.getClusterName(), entry.getShardName(), entry.getHostPort(),
                        ALERT_TYPE.SENTINEL_MONITOR_INCONSIS, getMessage());
            }
        }
        ;

        public static SentinelCollectorAction getAction(boolean masterGood, boolean monitorGood) {
            if(!masterGood && !monitorGood) {
                return MASTER_BAD_MONITOR_BAD;
            }
            if(!monitorGood) {
                return MASTER_GOOD_MONITOR_BAD;
            }
            if(!masterGood) {
                return MASTER_BAD_MONITOR_GOOD;
            }

            return MASTER_GOOD_MONITOR_GOOD;

        }

        protected abstract String getMessage();

        protected abstract void doAction(SentinelCollector4Keeper collector, SentinelHello hello, ClusterShardHostPort entry);

    }
}
