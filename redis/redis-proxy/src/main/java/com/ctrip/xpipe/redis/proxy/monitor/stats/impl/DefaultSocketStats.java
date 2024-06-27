package com.ctrip.xpipe.redis.proxy.monitor.stats.impl;

import com.ctrip.xpipe.redis.core.proxy.monitor.SocketStatsResult;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.monitor.stats.AbstractStats;
import com.ctrip.xpipe.redis.proxy.monitor.stats.SocketStatsManager;
import com.ctrip.xpipe.redis.proxy.monitor.stats.SocketStats;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.google.common.collect.Lists;
import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public class DefaultSocketStats extends AbstractStats implements SocketStats {

    private int localPort = -1, remotePort = -1;

    private static final SocketStatsResult EMPTY_ONE = new SocketStatsResult(Lists.newArrayList("Empty"));

    private SocketStatsManager socketStatsManager;

    private AtomicReference<SocketStatsResult> result = new AtomicReference<>(EMPTY_ONE);

    public DefaultSocketStats(Session session, ScheduledExecutorService scheduled, SocketStatsManager socketStatsManager) {
        super(session, scheduled);
        this.socketStatsManager = socketStatsManager;
    }

    @Override
    public SocketStatsResult getSocketStatsResult() {
        return result.get();
    }

    @Override
    protected void doTask() {
        Channel channel = getSession().getChannel();
        if(channel == null || !channel.isActive()) {
            logger.debug("[doTask] Channel null");
            return;
        }
        if(localPort == -1 && remotePort == -1) {
            localPort = ((InetSocketAddress) channel.localAddress()).getPort();
            remotePort = ((InetSocketAddress) channel.remoteAddress()).getPort();
        }

        SocketStatsResult socketStatsResult = socketStatsManager.getSocketStatsResult(localPort, remotePort);
        if (socketStatsResult == null) {
            logger.warn("[doTask] fail to get socket stat of channel:{}", ChannelUtil.getDesc(channel));
            result.set(EMPTY_ONE);
        } else {
            logger.debug("[doTask] succeed to get socket stat of channel:{}", ChannelUtil.getDesc(channel));
            result.set(socketStatsResult);
        }
    }
}
