package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.listener;

import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractMetricListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.CrdtInfoReplicationContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.CrdtInfoReplicationListener;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;
import org.springframework.stereotype.Component;

@Component
public class PeerBacklogOffsetListener extends AbstractMetricListener<CrdtInfoReplicationContext, HealthCheckAction>  implements CrdtInfoReplicationListener, BiDirectionSupport {
    
    public static final String METRIC_TYPE = "crdt.peer.backlog_offset";
    
    @Override
    public void onAction(CrdtInfoReplicationContext context) {
        try {
            RedisInstanceInfo info = context.instance().getCheckInfo();
            if(info.isMaster()) {
                CRDTInfoResultExtractor extractor = (CRDTInfoResultExtractor)context.getResult();
                long offset = extractor.getMasterReplOffset();
                long recvTimeMilli = context.getRecvTimeMilli();
                tryWriteMetric(getPoint(METRIC_TYPE, offset, recvTimeMilli, info));
            }
        } catch (Throwable throwable) {
            logger.warn("[onAction] redis:{} error: ",context.instance().getCheckInfo().getHostPort(), throwable);
        }
    }

}
