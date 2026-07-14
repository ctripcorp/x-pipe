package com.ctrip.xpipe.redis.console.healthcheck.nonredis.clusterstatus;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import com.ctrip.xpipe.redis.core.beacon.BeaconSentinelMetaUtil;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.ServicesUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Component
public class OperatingShardMonitor extends AbstractCrossDcIntervalAction {

    static final String CAT_TYPE = "operating.shard";

    static final String METRIC_TYPE = "operating.shard";

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private ConsoleCommonConfig commonConfig;

    private MetricProxy metricProxy = ServicesUtil.getMetricProxy();

    @Override
    protected void doAction() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (xpipeMeta == null || xpipeMeta.getDcs() == null) {
            logger.info("[doAction][{}] report 0 operating shards", METRIC_TYPE);
            return;
        }

        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (dcMeta.getClusters() == null) {
                continue;
            }
            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                if (clusterMeta.getShards() == null) {
                    continue;
                }
                for (Map.Entry<String, ShardMeta> entry : clusterMeta.getShards().entrySet()) {
                    if (BeaconSentinelMetaUtil.isOperatingExcluded(entry.getValue())) {
                        reportShard(dcMeta.getId(), clusterMeta.getId(), entry.getKey());
                    }
                }
            }
        }

        logger.info("[doAction][{}] report operating shards", METRIC_TYPE);
    }

    private void reportShard(String dcName, String clusterName, String shardName) {
        String dc = nullToEmpty(dcName);
        String cluster = nullToEmpty(clusterName);
        String shard = nullToEmpty(shardName);
        EventMonitor.DEFAULT.logEvent(CAT_TYPE, shard);
        try {
            metricProxy.writeBinMultiDataPoint(buildMetricData(dc, cluster, shard));
        } catch (MetricProxyException e) {
            logger.error("[reportShard][{}][{}][{}]", dc, cluster, shard, e);
        }
    }

    private MetricData buildMetricData(String dcName, String clusterName, String shardName) {
        MetricData metricData = new MetricData(METRIC_TYPE, dcName, clusterName, shardName);
        metricData.setTimestampMilli(System.currentTimeMillis());
        metricData.setValue(1);
        metricData.addTag("dcName", dcName);
        metricData.addTag("clusterName", clusterName);
        metricData.addTag("shardName", shardName);
        return metricData;
    }

    private String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    @Override
    protected long getIntervalMilli() {
        return commonConfig.getAbnormalClusterStatusMonitorIntervalMilli();
    }

    @Override
    protected long getLeastIntervalMilli() {
        return commonConfig.getAbnormalClusterStatusMonitorIntervalMilli();
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Collections.emptyList();
    }

    @VisibleForTesting
    void setMetricProxy(MetricProxy metricProxy) {
        this.metricProxy = metricProxy;
    }
}
