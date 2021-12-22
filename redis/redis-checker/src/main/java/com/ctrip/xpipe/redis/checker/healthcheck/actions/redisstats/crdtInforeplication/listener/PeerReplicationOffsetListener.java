package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.listener;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractMetricListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.CrdtInfoReplicationContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.CrdtInfoReplicationListener;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PeerReplicationOffsetListener extends AbstractMetricListener<CrdtInfoReplicationContext, HealthCheckAction>  implements CrdtInfoReplicationListener, BiDirectionSupport {

    static public final String METRIC_TYPE = "crdt.peer.replication_offset";
    
    @Autowired
    private MetaCache metaCache;
    
    public static final String KEY_SRC_PEER = "srcpeer";
    public static final String KEY_SRC_PEER_DC = "srcpeerdc";
    public static final String UNKNOWN_DC = "unknown";
    
    @Override
    public void onAction(CrdtInfoReplicationContext context) {
        try {
            RedisInstanceInfo info = context.instance().getCheckInfo();
            if(info.isMaster()) {
                CRDTInfoResultExtractor extractor = (CRDTInfoResultExtractor)context.getResult();
                long recvTimeMilli = context.getRecvTimeMilli();
                extractor.extractPeerMasters().forEach(peerInfo -> {
                    MetricData data = getPoint(METRIC_TYPE, peerInfo.getReplOffset(), recvTimeMilli, info);
                    HostPort peer = new HostPort(peerInfo.getEndpoint().getHost() , peerInfo.getEndpoint().getPort());
                    data.addTag(KEY_SRC_PEER, peer.toString());
                    String dc;
                    try {
                        dc = metaCache.getDc(peer);
                        data.addTag(KEY_SRC_PEER_DC, dc);
                    }catch (Exception ignore){
                        logger.debug("{} not find peer {} dc", info.getHostPort(), peer);
                        data.addTag(KEY_SRC_PEER_DC, UNKNOWN_DC);
                    }
                    tryWriteMetric(data);
                });
            }
            
        } catch (Throwable throwable) {
          logger.error("[onAction] {}", context.instance().getCheckInfo().getHostPort(), throwable);  
        }
        
    }

    @VisibleForTesting
    public void setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
    }
}
