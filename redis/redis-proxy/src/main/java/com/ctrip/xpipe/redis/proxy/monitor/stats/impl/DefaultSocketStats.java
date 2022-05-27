package com.ctrip.xpipe.redis.proxy.monitor.stats.impl;

import com.ctrip.xpipe.redis.core.proxy.monitor.SocketStatsResult;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.monitor.stats.AbstractStats;
import com.ctrip.xpipe.redis.proxy.monitor.stats.SocketStats;
import com.ctrip.xpipe.utils.AbstractScriptExecutor;
import com.google.common.collect.Lists;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public class DefaultSocketStats extends AbstractStats implements SocketStats {

    private Session session;

    private int localPort = -1, remotePort = -1;

    private static final SocketStatsResult EMPTY_ONE = new SocketStatsResult(Lists.newArrayList("Empty"));

    private AtomicReference<SocketStatsResult> result = new AtomicReference<>(EMPTY_ONE);

    public DefaultSocketStats(ScheduledExecutorService scheduled, Session session) {
        super(scheduled);
        this.session = session;
    }

    @Override
    public SocketStatsResult getSocketStatsResult() {
        return result.get();
    }

    @Override
    protected void doTask() {
        Channel channel = session.getChannel();
        if(channel == null || !channel.isActive()) {
            logger.warn("[doTask] Channel null");
            return;
        }
        if(localPort == -1 && remotePort == -1) {
            localPort = ((InetSocketAddress) channel.localAddress()).getPort();
            remotePort = ((InetSocketAddress) channel.remoteAddress()).getPort();
        }

        new SocketStatsScriptExecutor(localPort, remotePort)
                .execute()
                .addListener(future -> result.set(future.getNow()));
    }

    private static class SocketStatsScriptExecutor extends AbstractScriptExecutor<SocketStatsResult> {

        private static final String SS_TEMPLATE = "ss -itnm '( sport = :%d and dport = :%d )' | grep -v State";

        private static Logger logger = LoggerFactory.getLogger(SocketStatsScriptExecutor.class);

        private int localPort;

        private int remotePort;

        public SocketStatsScriptExecutor(int localPort, int remotePort) {
            this.localPort = localPort;
            this.remotePort = remotePort;
        }

        @Override
        public String getScript() {
            return String.format(SS_TEMPLATE, localPort, remotePort);
        }

        @Override
        public SocketStatsResult format(List<String> result) {
            return new SocketStatsResult(result);
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }
    }
}
