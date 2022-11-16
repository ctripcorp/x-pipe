package com.ctrip.xpipe.redis.proxy.handler.response;

import com.ctrip.xpipe.redis.core.protocal.error.ProxyError;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.monitor.*;
import com.ctrip.xpipe.redis.core.proxy.parser.monitor.ProxyMonitorParser;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.monitor.session.SessionStats;
import com.ctrip.xpipe.redis.proxy.monitor.stats.PingStats;
import com.ctrip.xpipe.redis.proxy.monitor.stats.PingStatsManager;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import com.google.common.collect.Lists;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProxyMonitorHandler extends AbstractProxyProtocolOptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ProxyMonitorHandler.class);

    private TunnelManager tunnelManager;

    private PingStatsManager pingStatsManager;

    private ProxyConfig proxyConfig;

    public ProxyMonitorHandler(TunnelManager tunnelManager, PingStatsManager pingStatsManager, ProxyConfig proxyConfig) {
        super(()->proxyConfig.getResponseTimeout());
        this.tunnelManager = tunnelManager;
        this.pingStatsManager = pingStatsManager;
        this.proxyConfig = proxyConfig;
    }

    @Override
    protected void doHandle(Channel channel, String[] content) {
        if(content == null || content.length < 1) {
            throw new IllegalArgumentException("monitor option is needed");
        }
        ProxyMonitorParser.Type type = ProxyMonitorParser.Type.parse(content[0]);
        if(type == null) {
            logger.warn("[doHandle] unknown type: {}", content[0]);
            return;
        }

        switch (type) {
            case SocketStats:
                new SocketStatsResponser().response(channel);
                break;
            case PingStats:
                new PingStatsResponser().response(channel);
                break;
            case TunnelStats:
                new TunnelStatsResponser().response(channel);
                break;
            case TrafficStats:
                new TunnelTrafficResponser().response(channel);
                break;
            default:
                break;
        }
    }

    @Override
    public PROXY_OPTION getOption() {
        return PROXY_OPTION.MONITOR;
    }


    private abstract class AbstractResponser<T> implements Responser {

        @Override
        public void response(Channel channel) {
            try {
                List<T> samples = getSamples();
                Object[] resultSet = new Object[samples.size()];
                int index = 0;
                for (T t : samples) {
                    Object result = format(t);
                    if (result != null)
                        resultSet[index++] = result;
                }
                if (index == 0) {
                    resultSet = new Object[0];
                }

                channel.writeAndFlush(new ArrayParser(resultSet).format());
            } catch (Throwable t) {
                logger.warn("[response] {}", channel, t);
                channel.writeAndFlush(new RedisErrorParser(new ProxyError(t.getClass().getName() + ": " + t.getMessage())).format());
            }
        }

        abstract List<T> getSamples();

        abstract Object format(T t);
    }


    private class SocketStatsResponser extends AbstractResponser<Tunnel> {

        @Override
        protected List<Tunnel> getSamples() {
            return tunnelManager.tunnels();
        }

        @Override
        protected Object format(Tunnel tunnel) {
            String tunnelId = tunnel.identity().toString();
            if (tunnel.getTunnelMonitor() == null || tunnel.getTunnelMonitor().getBackendSessionMonitor() == null
                    || tunnel.getTunnelMonitor().getFrontendSessionMonitor() == null) {
                return null;
            }
            SocketStatsResult frontendSocketStats = tunnel.getTunnelMonitor().getFrontendSessionMonitor()
                    .getSocketStats().getSocketStatsResult();
            SocketStatsResult backendSocketStats = tunnel.getTunnelMonitor().getBackendSessionMonitor()
                    .getSocketStats().getSocketStatsResult();
            TunnelSocketStatsResult result = new TunnelSocketStatsResult(tunnelId, frontendSocketStats, backendSocketStats);
            return result.format();
        }

    }

    private class PingStatsResponser extends AbstractResponser<PingStats> {

        @Override
        List<PingStats> getSamples() {
            return Lists.newArrayList(pingStatsManager.getAllPingStats());
        }

        @Override
        Object format(PingStats pingStats) {
            return pingStats.getPingStatsResult().toArrayObject();
        }
    }

    private class TunnelStatsResponser extends AbstractResponser<Tunnel> {

        @Override
        List<Tunnel> getSamples() {
            return tunnelManager.tunnels();
        }

        @Override
        Object format(Tunnel tunnel) {
            if (tunnel.getTunnelMonitor() == null || tunnel.getTunnelMonitor().getTunnelStats() == null) {
                return null;
            }
            TunnelStatsResult tunnelStatsResult = tunnel.getTunnelMonitor().getTunnelStats().getTunnelStatsResult();
            return  tunnelStatsResult == null ? null :tunnelStatsResult.toArrayObject();
        }
    }

    private class TunnelTrafficResponser extends AbstractResponser<Tunnel> {

        @Override
        protected List<Tunnel> getSamples() {
            return tunnelManager.tunnels();
        }

        @Override
        protected Object format(Tunnel tunnel) {
            String tunnelId = tunnel.identity().toString();
            if (tunnel.getTunnelMonitor() == null || tunnel.getTunnelMonitor().getBackendSessionMonitor() == null
                    || tunnel.getTunnelMonitor().getFrontendSessionMonitor() == null) {
                return null;
            }
            SessionStats frontendStats = tunnel.getTunnelMonitor().getFrontendSessionMonitor().getSessionStats();
            SessionStats backendStats = tunnel.getTunnelMonitor().getBackendSessionMonitor().getSessionStats();
            SessionTrafficResult frontend = new SessionTrafficResult(frontendStats.lastUpdateTime(),
                    frontendStats.getInputBytes(), frontendStats.getOutputBytes(), frontendStats.getInputInstantaneousBPS(), frontendStats.getOutputInstantaneousBPS());
            SessionTrafficResult backend = new SessionTrafficResult(backendStats.lastUpdateTime(),
                    backendStats.getInputBytes(), backendStats.getOutputBytes(), backendStats.getInputInstantaneousBPS(), backendStats.getOutputInstantaneousBPS());
            return new TunnelTrafficResult(tunnelId, frontend, backend).format();
        }
    }
}
