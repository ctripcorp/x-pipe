package com.ctrip.xpipe.redis.console.healthcheck.delay;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.console.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.utils.ServicesUtil;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;


/**
 * @author chen.zhu
 * <p>
 * Aug 29, 2018
 */
@Component
public class MetricDelayListener implements HealthCheckActionListener<DelayActionContext> {

    private static final Logger logger = LoggerFactory.getLogger(MetricDelayListener.class);

    private static final String TYPE = "delay";

    private MetricProxy proxy = ServicesUtil.getMetricProxy();

    private MetricData getPoint(DelayActionContext context) {
        RedisHealthCheckInstance instance = context.instance();
        MetricData data = new MetricData(TYPE, instance.getRedisInstanceInfo().getClusterId(),
                instance.getRedisInstanceInfo().getShardId());
        data.setValue(context.getResult() / 1000);
        data.setTimestampMilli(System.currentTimeMillis());
        data.setHostPort(instance.getRedisInstanceInfo().getHostPort());
        return data;
    }

    @Override
    public void onAction(DelayActionContext delayActionContext) {
        try {
            proxy.writeBinMultiDataPoint(getPoint(delayActionContext));
        } catch (Exception e) {
            logger.error("Error send metrics to metric", e);
        }
    }

    @Override
    public boolean worksfor(ActionContext t) {
        return t instanceof DelayActionContext;
    }
}
