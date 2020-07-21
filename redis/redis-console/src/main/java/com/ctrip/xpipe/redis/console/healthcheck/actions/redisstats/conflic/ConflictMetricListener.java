package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.conflic;

import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.AbstractMetricListener;
import org.springframework.stereotype.Component;

@Component
public class ConflictMetricListener extends AbstractMetricListener<CrdtConflictCheckContext> implements ConflictCheckListener, BiDirectionSupport {

    protected static final String METRIC_TYPE_CONFLICT = "crdt.conflict.type";
    protected static final String METRIC_NON_TYPE_CONFLICT = "crdt.conflict.nontype";
    protected static final String METRIC_MODIFY_CONFLICT = "crdt.conflict.modify";
    protected static final String METRIC_MERGE_CONFLICT = "crdt.conflict.merge";

    @Override
    public void onAction(CrdtConflictCheckContext context) {
        CrdtConflictStats conflictStats = context.getResult();

        long recvTimeMilli = context.getRecvTimeMilli();
        RedisInstanceInfo info = context.instance().getRedisInstanceInfo();
        tryWriteMetric(getPoint(METRIC_TYPE_CONFLICT, conflictStats.getTypeConflict(), recvTimeMilli, info));
        tryWriteMetric(getPoint(METRIC_NON_TYPE_CONFLICT, conflictStats.getNonTypeConflict(), recvTimeMilli, info));
        tryWriteMetric(getPoint(METRIC_MODIFY_CONFLICT, conflictStats.getModifyConflict(), recvTimeMilli, info));
        tryWriteMetric(getPoint(METRIC_MERGE_CONFLICT, conflictStats.getMergeConflict(), recvTimeMilli, info));
    }

}
