package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperdelay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.keeperstate.KeeperStateCache;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
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

    private static final String TAG_ROLE_UNKNOWN = "unknown";

    @Autowired
    private FoundationService foundationService;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private KeeperStateCache keeperStateCache;

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
        data.addTag("role", role(keeperStateCache.getState(info.getHostPort())));
        return data;
    }

    /**
     * 取打点时刻 {@link KeeperStateCache} 中的角色，未采到或已过期打 unknown。
     *
     * <p>ACTIVE / BACKUP 之外还有 PREPARE、PRE_ACTIVE 等过渡态，它们既非 active 也非 backup，
     * 原样打出比归到两者任一更忠实。
     */
    private String role(KeeperState state) {
        return null == state ? TAG_ROLE_UNKNOWN : state.name().toLowerCase();
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
