package com.ctrip.xpipe.redis.console.healthcheck.nonredis.clusterstatus;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.utils.ServicesUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Component
public class OperatingShardMonitor extends AbstractCrossDcIntervalAction {

    static final String CAT_TYPE = "operating.shard";

    static final String METRIC_TYPE = "operating.shard";

    @Autowired
    private DcClusterShardService dcClusterShardService;

    private MetricProxy metricProxy = ServicesUtil.getMetricProxy();

    @Override
    protected void doAction() {
        List<DcClusterShardTbl> operatingShards = dcClusterShardService.findOperatingDcClusterShards();
        if (operatingShards == null || operatingShards.isEmpty()) {
            logger.info("[doAction][{}] report 0 operating shards", METRIC_TYPE);
            return;
        }
        for (DcClusterShardTbl shard : operatingShards) {
            reportShard(shard);
        }
        logger.info("[doAction][{}] report {} operating shards", METRIC_TYPE, operatingShards.size());
    }

    private void reportShard(DcClusterShardTbl shard) {
        String dcName = nullToEmpty(shard.getDcName());
        String clusterName = nullToEmpty(shard.getClusterName());
        String shardName = nullToEmpty(shard.getShardName());
        EventMonitor.DEFAULT.logEvent(CAT_TYPE, shardName);
        try {
            metricProxy.writeBinMultiDataPoint(buildMetricData(dcName, clusterName, shardName));
        } catch (MetricProxyException e) {
            logger.error("[reportShard][{}][{}][{}]", dcName, clusterName, shardName, e);
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
        return consoleConfig.getAbnormalClusterStatusMonitorIntervalMilli();
    }

    @Override
    protected long getLeastIntervalMilli() {
        return consoleConfig.getAbnormalClusterStatusMonitorIntervalMilli();
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
