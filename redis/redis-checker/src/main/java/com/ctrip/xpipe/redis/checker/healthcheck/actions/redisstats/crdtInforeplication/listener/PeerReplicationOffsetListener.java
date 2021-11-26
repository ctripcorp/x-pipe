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
    
    @Override
    public void onAction(CrdtInfoReplicationContext context) {
        InfoResultExtractor extractor = context.getResult();
        for(int i = 0; i < 16;i++) {
            String prefix = String.format("peer%d_", i);
            String peerHost = extractor.extract(prefix + "host");
            if(peerHost != null) {
                String peerPort = extractor.extract(prefix + "port");
                long peerReplOffset = extractor.extractAsLong(prefix + "repl_offset");
                long recvTimeMilli = context.getRecvTimeMilli();
                RedisInstanceInfo info = context.instance().getCheckInfo();
                MetricData data = getPoint(METRIC_TYPE, peerReplOffset, recvTimeMilli, info);
                HostPort peer = new HostPort(peerHost , Integer.parseInt(peerPort));
                data.addTag(KEY_SRC_PEER, peer.toString());
                String dc;
                try {
                    dc = metaCache.getDc(peer);
                    data.addTag(KEY_SRC_PEER_DC, dc);
                }catch (Exception ignore){
                    logger.debug("{} not find peer {} dc", info.getHostPort(), peer);
                }
                tryWriteMetric(data);
            }
        }
    }

    @VisibleForTesting
    public void setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
    }
}
