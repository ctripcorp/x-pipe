package com.ctrip.xpipe.redis.core.proxy.command;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.payload.InOutPayloadFactory;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.exception.XPipeProxyResultException;
import com.ctrip.xpipe.redis.core.proxy.monitor.PingStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelSocketStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelTrafficResult;
import com.ctrip.xpipe.redis.core.proxy.parser.monitor.ProxyMonitorParser;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Oct 26, 2018
 */
public abstract class AbstractProxyMonitorCommand<T> extends AbstractProxyCommand<T[]> {

    private static final String MONITOR_PREFIX = String.format("%s %s", ProxyProtocol.KEY_WORD, PROXY_OPTION.MONITOR.name());

    private static final int PROXY_CONNECTION_TIMEOUT_MILLI = Integer.parseInt(System.getProperty("proxy.connection.timeout.milli", "5000"));

    public AbstractProxyMonitorCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
        setInOutPayloadFactory(new InOutPayloadFactory.DirectByteBufInOutPayloadFactory());
    }

    public AbstractProxyMonitorCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
                                       int commandTimeoutMilli) {
        super(clientPool, scheduled, commandTimeoutMilli);
        setInOutPayloadFactory(new InOutPayloadFactory.DirectByteBufInOutPayloadFactory());
    }

    @Override
    public ByteBuf getRequest() {
        return new SimpleStringParser(String.format("%s %s", MONITOR_PREFIX, getType().name())).format();
    }

    @Override
    protected T[] format(Object payload) {
        if(!payload.getClass().isArray()) {
            throw new XPipeProxyResultException(getClass().getSimpleName() + ": result should be an array");
        }
        Object[] objects = (Object[]) payload;
        T[] result = initArray(objects);
        int index = 0;
        for(Object object : objects) {
            result[index ++] = parseObject(object);
        }
        return result;
    }

    @Override
    public int getCommandTimeoutMilli() {
        return PROXY_CONNECTION_TIMEOUT_MILLI;
    }

    @Override
    protected boolean logRequest() {
        return false;
    }

    @Override
    protected boolean logResponse() {
        return false;
    }

    protected abstract T[] initArray(Object[] objects);

    protected abstract T parseObject(Object object);

    protected abstract ProxyMonitorParser.Type getType();


    public static class ProxyMonitorSocketStatsCommand extends AbstractProxyMonitorCommand<TunnelSocketStatsResult> {

        public ProxyMonitorSocketStatsCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
            super(clientPool, scheduled);
        }

        public ProxyMonitorSocketStatsCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
            super(clientPool, scheduled, commandTimeoutMilli);
        }

        @Override
        protected TunnelSocketStatsResult[] initArray(Object[] objects) {
            return new TunnelSocketStatsResult[objects.length];
        }

        @Override
        protected TunnelSocketStatsResult parseObject(Object object) {
            return TunnelSocketStatsResult.parse(object);
        }

        @Override
        protected ProxyMonitorParser.Type getType() {
            return ProxyMonitorParser.Type.SocketStats;
        }

    }

    public static class ProxyMonitorTunnelStatsCommand extends AbstractProxyMonitorCommand<TunnelStatsResult> {

        public ProxyMonitorTunnelStatsCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
            super(clientPool, scheduled);
        }

        public ProxyMonitorTunnelStatsCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
            super(clientPool, scheduled, commandTimeoutMilli);
        }

        @Override
        protected TunnelStatsResult[] initArray(Object[] objects) {
            return new TunnelStatsResult[objects.length];
        }

        @Override
        protected TunnelStatsResult parseObject(Object object) {
            return TunnelStatsResult.parse(object);
        }

        @Override
        protected ProxyMonitorParser.Type getType() {
            return ProxyMonitorParser.Type.TunnelStats;
        }

    }

    public static class ProxyMonitorPingStatsCommand extends AbstractProxyMonitorCommand<PingStatsResult> {

        public ProxyMonitorPingStatsCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
            super(clientPool, scheduled);
        }

        public ProxyMonitorPingStatsCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
            super(clientPool, scheduled, commandTimeoutMilli);
        }

        @Override
        protected PingStatsResult[] initArray(Object[] objects) {
            return new PingStatsResult[objects.length];
        }

        @Override
        protected PingStatsResult parseObject(Object object) {
            return PingStatsResult.parse(object);
        }

        @Override
        protected ProxyMonitorParser.Type getType() {
            return ProxyMonitorParser.Type.PingStats;
        }

    }

    public static class ProxyMonitorTrafficStatsCommand extends AbstractProxyMonitorCommand<TunnelTrafficResult> {

        public ProxyMonitorTrafficStatsCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
            super(clientPool, scheduled);
        }

        public ProxyMonitorTrafficStatsCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
            super(clientPool, scheduled, commandTimeoutMilli);
        }

        @Override
        protected TunnelTrafficResult[] initArray(Object[] objects) {
            return new TunnelTrafficResult[objects.length];
        }

        @Override
        protected TunnelTrafficResult parseObject(Object object) {
            return TunnelTrafficResult.parse(object);
        }

        @Override
        protected ProxyMonitorParser.Type getType() {
            return ProxyMonitorParser.Type.TrafficStats;
        }

    }
}
