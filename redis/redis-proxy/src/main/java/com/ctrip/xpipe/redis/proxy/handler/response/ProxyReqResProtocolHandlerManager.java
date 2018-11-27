package com.ctrip.xpipe.redis.proxy.handler.response;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.proxy.monitor.stats.PingStatsManager;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Maps;
import io.netty.channel.Channel;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public class ProxyReqResProtocolHandlerManager implements ProxyProtocolOptionHandler {

    private ResourceManager resourceManager;

    private TunnelManager tunnelManager;

    private PingStatsManager pingStatsManager;

    private Map<PROXY_OPTION, ProxyProtocolOptionHandler> handlers = Maps.newConcurrentMap();

    private final static ExecutorService sequentialExecutor = Executors.newSingleThreadExecutor(
            XpipeThreadFactory.create("ProxyReqResProtocolHandler"));

    public ProxyReqResProtocolHandlerManager(ResourceManager resourceManager, TunnelManager tunnelManager,
                                             PingStatsManager pingStatsManager) {
        this.resourceManager = resourceManager;
        this.tunnelManager = tunnelManager;
        this.pingStatsManager = pingStatsManager;
        init();
    }

    private void init() {
        putHandler(new ProxyPingHandler(resourceManager));
        putHandler(new ProxyMonitorHandler(tunnelManager, pingStatsManager, resourceManager.getProxyConfig()));
    }

    @Override
    public PROXY_OPTION getOption() {
        return null;
    }

    @Override
    public void handle(Channel channel, String[] content) {
        sequentiallyExecute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                getHandler(content[0]).handle(channel, content);
            }
        });
    }

    private void sequentiallyExecute(Runnable run) {
        sequentialExecutor.execute(run);
    }

    private void putHandler(ProxyProtocolOptionHandler handler) {
        handlers.put(handler.getOption(), handler);
    }

    private ProxyProtocolOptionHandler getHandler(String content) {
        return handlers.get(PROXY_OPTION.valueOf(content));
    }
}
