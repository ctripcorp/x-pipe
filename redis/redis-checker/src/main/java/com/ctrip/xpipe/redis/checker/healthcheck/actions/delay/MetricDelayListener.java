package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.checker.config.impl.CommonConfigBean;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.utils.ServicesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;


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

    private static final long MILLIS_PER_MINUTE = 60_000L;

    private static final String TAG_IS_NEW_YES = "1";

    private static final String TAG_IS_NEW_NO = "0";

    @Autowired
    private FoundationService foundationService;

    @Autowired
    private CommonConfigBean commonConfigBean;

    private MetricProxy proxy = ServicesUtil.getMetricProxy();

    private MetricData getPoint(DelayActionContext context) {
        RedisInstanceInfo info = context.instance().getCheckInfo();

        MetricData data = new MetricData(TYPE, info.getDcId(), info.getClusterId(), info.getShardId());
        data.setValue(context.getResult() / THOUSAND);
        data.setTimestampMilli(context.getRecvTimeMilli());
        data.setHostPort(info.getHostPort());

        data.setClusterType(info.getClusterType());
        Date createTime = info.getCreateTime();
        int minutes = commonConfigBean.getDelayMetricNewInstanceMinutes();
        long recvTime = context.getRecvTimeMilli();
        boolean isNew = isNewInstance(createTime, recvTime, minutes);
        data.addTag("isNew", isNew ? TAG_IS_NEW_YES : TAG_IS_NEW_NO);
        data.addTag("crossDc", String.valueOf(!foundationService.getDataCenter().equalsIgnoreCase(info.getDcId())));
        data.addTag("crossRegion", String.valueOf(info.isCrossRegion()));
        if (context instanceof HeteroDelayActionContext) {
            data.addTag("srcShardId", String.valueOf(((HeteroDelayActionContext) context).getShardDbId()));
        } else {
            data.addTag("srcShardId", "-");
        }
        return data;
    }

    static boolean isNewInstance(Date createTime, long recvTimeMilli, int windowMinutes) {
        if (createTime == null) {
            return false;
        }
        return recvTimeMilli < createTime.getTime() + windowMinutes * MILLIS_PER_MINUTE;
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
