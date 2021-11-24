package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.listener;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
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
public class CrdtPeerBacklogOffsetListener extends AbstractMetricListener<CrdtInfoReplicationContext, HealthCheckAction>  implements CrdtInfoReplicationListener, BiDirectionSupport {

    static public final String METRIC_TYPE = "redis.peer.backlog.offset";
    
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
                data.setHostPort(new HostPort(peerHost, Integer.parseInt(peerPort)));
                tryWriteMetric(data);
            }
        }
    }

}
