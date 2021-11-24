package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.listener;

import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractMetricListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.CrdtInfoReplicationContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.CrdtInfoReplicationListener;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.stereotype.Component;

@Component
public class CrdtBacklogOffsetListener extends AbstractMetricListener<CrdtInfoReplicationContext, HealthCheckAction>  implements CrdtInfoReplicationListener, BiDirectionSupport {
    
    public static final String METRIC_TYPE = "redis.crdt.backlog.offset";
    @Override
    public void onAction(CrdtInfoReplicationContext context) {
        InfoResultExtractor extractor = context.getResult();
        long offset = extractor.extractAsLong("master_repl_offset");
        long recvTimeMilli = context.getRecvTimeMilli();
        RedisInstanceInfo info = context.instance().getCheckInfo();
        tryWriteMetric(getPoint(METRIC_TYPE, offset, recvTimeMilli, info));
    }

}
