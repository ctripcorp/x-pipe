package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats.listener;

import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractMetricListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats.CrdtInfoStatsContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats.CrdtInfoStatsListener;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.springframework.stereotype.Component;

@Component
public class CrdtSyncListener extends AbstractMetricListener<CrdtInfoStatsContext, HealthCheckAction> implements CrdtInfoStatsListener, BiDirectionSupport {
    public static final String METRIC_TYPE_SYNC_FULL = "crdt.sync.full";
    
    public static final String METRIC_TYPE_SYNC_PARTIAL_OK = "crdt.sync.partial_ok";
    
    public static final String METRIC_TYPE_SYNC_PARTIAL_ERR = "crdt.sync.partial_err";
   
    @Override
    public void onAction(CrdtInfoStatsContext context) {
        InfoResultExtractor extractor = context.getResult();
        long recvTimeMilli = context.getRecvTimeMilli();
        RedisInstanceInfo info = context.instance().getCheckInfo();
        
        SyncStats stats = new SyncStats(extractor);
        
        tryWriteMetric(getPoint(METRIC_TYPE_SYNC_FULL, stats.getSyncFull(), recvTimeMilli, info));
        
        tryWriteMetric(getPoint(METRIC_TYPE_SYNC_PARTIAL_OK, stats.getSyncPartialOk(), recvTimeMilli, info));
        
        tryWriteMetric(getPoint(METRIC_TYPE_SYNC_PARTIAL_ERR, stats.getSyncPartialErr(), recvTimeMilli, info));
    }

    public static class SyncStats {
        private Long syncFull;

        private Long syncPartialOk;
        
        private Long syncPartialErr;

        private static final String KEY_SYNC_FULL = "sync_full";
        private static final String KEY_SYNC_PARTIAL_OK = "sync_partial_ok";
        private static final String KEY_SYNC_PARTIAL_ERR = "sync_partial_err";
        
        public SyncStats(InfoResultExtractor extractor) {
            syncFull = extractor.extractAsLong(KEY_SYNC_FULL);
            syncPartialOk = extractor.extractAsLong(KEY_SYNC_PARTIAL_OK);
            syncPartialErr = extractor.extractAsLong(KEY_SYNC_PARTIAL_ERR);
        }

        public Long getSyncFull() {
            return syncFull;
        }

        public Long getSyncPartialErr() {
            return syncPartialErr;
        }

        public Long getSyncPartialOk() {
            return syncPartialOk;
        }
    }
}
