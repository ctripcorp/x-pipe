package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperdelay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.ServicesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class KeeperMetricDelayListener implements KeeperDelayActionListener {

    private static final Logger logger = LoggerFactory.getLogger(KeeperMetricDelayListener.class);

    private static final String METRIC_TYPE = "delay";

    private static final double NANOS_PER_MICRO = 1000.0;

    private static final String TAG_TYPE_KEEPER = "keeper";

    private static final String TAG_IS_NEW_NO = "0";

    @Autowired
    private FoundationService foundationService;

    @Autowired
    private MetaCache metaCache;

    private MetricProxy proxy = ServicesUtil.getMetricProxy();

    private MetricData getPoint(KeeperDelayActionContext context) {
        KeeperInstanceInfo info = context.instance().getCheckInfo();

        MetricData data = new MetricData(METRIC_TYPE, info.getDcId(), info.getClusterId(), info.getShardId());
        data.setValue(context.getResult() / NANOS_PER_MICRO);
        data.setTimestampMilli(context.getRecvTimeMilli());
        data.setHostPort(info.getHostPort());
        data.setClusterType(info.getClusterType());
        data.addTag("type", TAG_TYPE_KEEPER);
        data.addTag("isNew", TAG_IS_NEW_NO);
        data.addTag("crossDc", String.valueOf(
                !foundationService.getDataCenter().equalsIgnoreCase(info.getDcId())));
        data.addTag("crossRegion", String.valueOf(metaCache.isCrossRegion(info.getActiveDc(), info.getDcId())));
        return data;
    }

    @Override
    public void onAction(KeeperDelayActionContext context) {
        try {
            proxy.writeBinMultiDataPoint(getPoint(context));
        } catch (Exception e) {
            logger.error("Error send keeper delay metrics to metric", e);
        }
    }

    @Override
    public void stopWatch(HealthCheckAction<KeeperHealthCheckInstance> action) {
        // do nothing
    }

    @Override
    public boolean supportInstance(KeeperHealthCheckInstance instance) {
        return true;
    }
}
