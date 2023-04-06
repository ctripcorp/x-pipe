package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.utils.ServicesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


/**
 * @author chen.zhu
 * <p>
 * Aug 29, 2018
 */
@Component
public class MetricDelayListener extends AbstractDelayActionListener implements DelayActionListener, OneWaySupport, BiDirectionSupport {

    private static final Logger logger = LoggerFactory.getLogger(MetricDelayListener.class);

    private static final String TYPE = "delay";

    private static final double THOUSAND = 1000.0;

    @Autowired
    private FoundationService foundationService;

    private MetricProxy proxy = ServicesUtil.getMetricProxy();

    private MetricData getPoint(DelayActionContext context) {
        RedisInstanceInfo info = context.instance().getCheckInfo();

        MetricData data = new MetricData(TYPE, info.getDcId(), info.getClusterId(), info.getShardId());
        data.setValue(context.getResult() / THOUSAND);
        data.setTimestampMilli(context.getRecvTimeMilli());
        data.setHostPort(info.getHostPort());

        data.setClusterType(info.getClusterType());
        data.addTag("delayType", context.getDelayType());
        data.addTag("crossDc", String.valueOf(foundationService.getDataCenter().equalsIgnoreCase(info.getDcId())));
        if (context instanceof HeteroDelayActionContext) {
            data.addTag("srcShardId", String.valueOf(((HeteroDelayActionContext) context).getShardDbId()));
        }
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
    public void stopWatch(HealthCheckAction action) {
        //do nothing
    }

    @Override
    public boolean supportInstance(RedisHealthCheckInstance instance) {
        return true;
    }
}
