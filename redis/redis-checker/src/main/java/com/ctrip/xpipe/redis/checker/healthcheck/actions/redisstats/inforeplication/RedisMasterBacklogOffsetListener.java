package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.inforeplication;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractMetricListener;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.springframework.stereotype.Component;

@Component
public class RedisMasterBacklogOffsetListener extends AbstractMetricListener<InfoReplicationContext, HealthCheckAction>
        implements InfoReplicationListener, OneWaySupport {

    protected static final String METRIC_TYPE_REDIS_MASTER_REPL_OFFSET = "redis.master.repl.offset";

    @Override
    public void onAction(InfoReplicationContext context) {
        try {
            RedisInstanceInfo info = context.instance().getCheckInfo();
            if(!info.isMaster()) return;

            InfoResultExtractor extractor = context.getResult();
            long offset = extractor.getMasterReplOffset();
            long recvTimeMilli = context.getRecvTimeMilli();

            tryWriteMetric(getPoint(METRIC_TYPE_REDIS_MASTER_REPL_OFFSET, offset, recvTimeMilli, info));

        } catch (Throwable th) {
            logger.error("get master backlog size of redis:{} error: ", context.instance().getCheckInfo().getHostPort(), th);
        }
    }
}
