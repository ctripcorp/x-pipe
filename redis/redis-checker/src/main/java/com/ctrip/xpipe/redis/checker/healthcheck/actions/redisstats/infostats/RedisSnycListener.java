package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.infostats;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractMetricListener;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.springframework.stereotype.Component;

@Component
public class RedisSnycListener extends AbstractMetricListener<InfoStatsContext, HealthCheckAction>
        implements InfoStatsListener, OneWaySupport {

    public static final String METRIC_TYPE_REDIS_SYNC_FULL = "redis.sync.full";

    public static final String METRIC_TYPE_REDIS_SYNC_PARTIAL_OK = "redis.sync.partial_ok";

    public static final String METRIC_TYPE_REDIS_SYNC_PARTIAL_ERR = "redis.sync.partial_err";

    @Override
    public void onAction(InfoStatsContext context) {
        try {
            InfoResultExtractor extractor = context.getResult();
            long recvTimeMilli = context.getRecvTimeMilli();
            RedisInstanceInfo info = context.instance().getCheckInfo();
            if(!info.isMaster()) return;

            tryWriteMetric(getPoint(METRIC_TYPE_REDIS_SYNC_FULL, extractor.getSyncFull(), recvTimeMilli, info));
            tryWriteMetric(getPoint(METRIC_TYPE_REDIS_SYNC_PARTIAL_OK, extractor.getSyncPartialOk(), recvTimeMilli, info));
            tryWriteMetric(getPoint(METRIC_TYPE_REDIS_SYNC_PARTIAL_ERR, extractor.getSyncPartialErr(), recvTimeMilli, info));
        } catch (Throwable throwable) {
            logger.error("get sync infos of redis:{} error: ", context.instance().getCheckInfo().getHostPort(), throwable);
        }
    }
}
