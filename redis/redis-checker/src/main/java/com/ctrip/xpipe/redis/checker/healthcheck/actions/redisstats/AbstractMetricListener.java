package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.utils.ServicesUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMetricListener<T extends ActionContext,  V extends HealthCheckAction> implements HealthCheckActionListener<T, V> {

    private MetricProxy proxy = ServicesUtil.getMetricProxy();

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void stopWatch(HealthCheckAction action) {
        // do nothing
    }

    protected void tryWriteMetric(MetricData point) {
        try {
            proxy.writeBinMultiDataPoint(point);
        } catch (Exception e) {
            logger.error("Error send metrics to metric", e);
        }
    }

    protected MetricData getPoint(String type, double value, long timestampMill, RedisInstanceInfo info) {
        MetricData data = new MetricData(type, info.getDcId(), info.getClusterId(), info.getShardId());
        data.setValue(value);
        data.setTimestampMilli(timestampMill);
        data.setHostPort(info.getHostPort());
        data.setClusterType(info.getClusterType());
        return data;
    }

    @VisibleForTesting
    public void setMetricProxy(MetricProxy proxy) {
        this.proxy = proxy;
    }

}
