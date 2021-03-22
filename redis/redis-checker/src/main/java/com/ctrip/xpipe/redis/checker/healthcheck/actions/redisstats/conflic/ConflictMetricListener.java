package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.conflic;

import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractMetricListener;
import org.springframework.stereotype.Component;

@Component
public class ConflictMetricListener extends AbstractMetricListener<CrdtConflictCheckContext, HealthCheckAction> implements ConflictCheckListener, BiDirectionSupport {

    protected static final String METRIC_TYPE_CONFLICT = "crdt.conflict.type";
    protected static final String METRIC_NON_TYPE_CONFLICT = "crdt.conflict.nontype";
    protected static final String METRIC_MODIFY_CONFLICT = "crdt.conflict.modify";
    protected static final String METRIC_MERGE_CONFLICT = "crdt.conflict.merge";
    protected static final String METRIC_SET_CONFLICT = "crdt.conflict.set";
    protected static final String METRIC_DEL_CONFLICT = "crdt.conflict.del";
    protected static final String METRIC_SET_DEL_CONFLICT = "crdt.conflict.setdel";

    @Override
    public void onAction(CrdtConflictCheckContext context) {
        CrdtConflictStats conflictStats = context.getResult();

        long recvTimeMilli = context.getRecvTimeMilli();
        RedisInstanceInfo info = context.instance().getCheckInfo();

        doConflictMetric(METRIC_TYPE_CONFLICT, conflictStats.getTypeConflict(), recvTimeMilli, info);
        doConflictMetric(METRIC_NON_TYPE_CONFLICT, conflictStats.getNonTypeConflict(), recvTimeMilli, info);
        doConflictMetric(METRIC_MODIFY_CONFLICT, conflictStats.getModifyConflict(), recvTimeMilli, info);
        doConflictMetric(METRIC_MERGE_CONFLICT, conflictStats.getMergeConflict(), recvTimeMilli, info);
        doConflictMetric(METRIC_SET_CONFLICT, conflictStats.getSetConflict(), recvTimeMilli, info);
        doConflictMetric(METRIC_DEL_CONFLICT, conflictStats.getDelConflict(), recvTimeMilli, info);
        doConflictMetric(METRIC_SET_DEL_CONFLICT, conflictStats.getSetDelConflict(), recvTimeMilli, info);
    }

    private void doConflictMetric(String type, Long conflict, long recvTimeMilli, RedisInstanceInfo info) {
        if (null == conflict) {
            logger.debug("[doConflictMetric] no val for conflict type {}, {}", type, info);
        } else {
            tryWriteMetric(getPoint(type, conflict, recvTimeMilli, info));
        }
    }

}
