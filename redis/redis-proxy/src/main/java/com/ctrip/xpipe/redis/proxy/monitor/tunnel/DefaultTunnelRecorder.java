package com.ctrip.xpipe.redis.proxy.monitor.tunnel;

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
        logger.info("{}", tunnel.identity().toString());
        logger.info("{}", tunnel.getTunnelMonitor().getTunnelStats().getTunnelStatsResult().toString());

        logger.info("{}:", SESSION_TYPE.FRONTEND.name());
        logger.info("{}\r\n", tunnel.getTunnelMonitor().getFrontendSessionMonitor().getOutboundBufferMonitor().getOutboundBufferCumulation());
        logger.info("{}\r\n", tunnel.getTunnelMonitor().getFrontendSessionMonitor().getSocketStats().getSocketStatsResult());
        logger.info("{}\r\n", tunnel.getTunnelMonitor().getFrontendSessionMonitor().getSessionStats());

        logger.info("{}:", SESSION_TYPE.BACKEND.name());
        logger.info("{}\r\n", tunnel.getTunnelMonitor().getBackendSessionMonitor().getOutboundBufferMonitor().getOutboundBufferCumulation());
        logger.info("{}\r\n", tunnel.getTunnelMonitor().getBackendSessionMonitor().getSocketStats().getSocketStatsResult());
        logger.info("{}\r\n", tunnel.getTunnelMonitor().getBackendSessionMonitor().getSessionStats());

        logger.info("{}", LINE_SPLITTER);
    }
}
