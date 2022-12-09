package com.ctrip.xpipe.redis.proxy.monitor.tunnel;

import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelRecorder;
import com.ctrip.xpipe.redis.proxy.session.SESSION_TYPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTunnelRecorder implements TunnelRecorder {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTunnelRecorder.class);

    private static final String LINE_SPLITTER = "================================================================";

    @Override
    public void record(Tunnel tunnel) {
        StringBuilder sb = new StringBuilder(tunnel.identity().toString());
        sb.append(RedisProtocol.CRLF);
        if (tunnel.getTunnelMonitor().getTunnelStats().getTunnelStatsResult() == null ){
            logger.info("there is no backend channel int tunnel : {}", tunnel.identity().toString());
            return;
        }
        sb.append(tunnel.getTunnelMonitor().getTunnelStats().getTunnelStatsResult().toString()).append(RedisProtocol.CRLF);

        sb.append(SESSION_TYPE.FRONTEND.name()).append(RedisProtocol.CRLF);
        sb.append("outbound buffer: ")
                .append(tunnel.getTunnelMonitor().getFrontendSessionMonitor().getOutboundBufferMonitor().getOutboundBufferCumulation())
                .append(RedisProtocol.CRLF);
        sb.append(tunnel.getTunnelMonitor().getFrontendSessionMonitor().getSocketStats().getSocketStatsResult().toString())
                .append(RedisProtocol.CRLF);
        sb.append(tunnel.getTunnelMonitor().getFrontendSessionMonitor().getSessionStats().toString()).append(RedisProtocol.CRLF);

        sb.append(SESSION_TYPE.BACKEND.name()).append(RedisProtocol.CRLF);
        sb.append("outbound buffer: ")
                .append(tunnel.getTunnelMonitor().getBackendSessionMonitor().getOutboundBufferMonitor().getOutboundBufferCumulation())
                .append(RedisProtocol.CRLF);
        sb.append(tunnel.getTunnelMonitor().getBackendSessionMonitor().getSocketStats().getSocketStatsResult().toString())
                .append(RedisProtocol.CRLF);
        sb.append(tunnel.getTunnelMonitor().getBackendSessionMonitor().getSessionStats().toString()).append(RedisProtocol.CRLF);

        sb.append(LINE_SPLITTER).append(RedisProtocol.CRLF);
        logger.info("{}", sb.toString());
    }
}
