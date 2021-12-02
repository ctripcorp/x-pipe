package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats.listener;

import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractMetricListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats.CrdtInfoStatsContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats.CrdtInfoStatsListener;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.springframework.stereotype.Component;

@Component
public class CrdtSyncListener extends AbstractMetricListener<CrdtInfoStatsContext, HealthCheckAction> implements CrdtInfoStatsListener, BiDirectionSupport {
    public static final String METRIC_TYPE_SYNC_FULL = "crdt.sync.full";
    
    public static final String METRIC_TYPE_SYNC_PARTIAL_OK = "crdt.sync.partial_ok";
    
    public static final String METRIC_TYPE_SYNC_PARTIAL_ERR = "crdt.sync.partial_err";
   
    @Override
    public void onAction(CrdtInfoStatsContext context) {
        try {
            CRDTInfoResultExtractor extractor = (CRDTInfoResultExtractor)context.getResult();
            long recvTimeMilli = context.getRecvTimeMilli();
            RedisInstanceInfo info = context.instance().getCheckInfo();
            tryWriteMetric(getPoint(METRIC_TYPE_SYNC_FULL, extractor.getSyncFull(), recvTimeMilli, info));
            tryWriteMetric(getPoint(METRIC_TYPE_SYNC_PARTIAL_OK, extractor.getSyncPartialOk(), recvTimeMilli, info));
            tryWriteMetric(getPoint(METRIC_TYPE_SYNC_PARTIAL_ERR, extractor.getSyncPartialErr(), recvTimeMilli, info));
        } catch (Throwable throwable) {
            
        }
        
    }
}
