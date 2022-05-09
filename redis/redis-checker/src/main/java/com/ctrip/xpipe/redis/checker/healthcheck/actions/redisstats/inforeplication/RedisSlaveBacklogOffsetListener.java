package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.inforeplication;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractMetricListener;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.springframework.stereotype.Component;

@Component
public class RedisSlaveBacklogOffsetListener extends AbstractMetricListener<InfoReplicationContext, HealthCheckAction>
        implements InfoReplicationListener, OneWaySupport {

    protected static final String METRIC_TYPE_REDIS_SLAVE_REPL_OFFSET = "redis.slave.repl.offset";

    @Override
    public void onAction(InfoReplicationContext context) {
        try {
            RedisInstanceInfo info = context.instance().getCheckInfo();
            if(info.isMaster()) return;

            InfoResultExtractor extractor = context.getResult();
            long recvTimeMilli = context.getRecvTimeMilli();
            long offset = extractor.getSlaveReplOffset();
            tryWriteMetric(getPoint(METRIC_TYPE_REDIS_SLAVE_REPL_OFFSET, offset, recvTimeMilli, info));

        } catch (Throwable th) {
            logger.error("get slave backlog size of redis:{} error: ", context.instance().getCheckInfo().getHostPort(), th);
        }
    }
}