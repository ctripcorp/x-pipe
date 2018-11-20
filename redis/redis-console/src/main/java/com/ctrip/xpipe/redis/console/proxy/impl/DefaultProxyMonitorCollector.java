package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollector;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;
import com.ctrip.xpipe.redis.core.proxy.command.AbstractProxyMonitorCommand;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.monitor.PingStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelSocketStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class DefaultProxyMonitorCollector extends AbstractStartStoppable implements ProxyMonitorCollector {

    private List<PingStatsResult> pingStatsResults;

    private List<TunnelStatsResult> tunnelStatsResults;

    private List<TunnelSocketStatsResult> socketStatsResults;

    private List<TunnelInfo> tunnelInfos;

    private ScheduledFuture future;

    private ScheduledExecutorService scheduled;

    private SimpleObjectPool<NettyClient> objectPool;

    private ProxyModel model;

    private List<Listener> listeners = Lists.newCopyOnWriteArrayList();

    public DefaultProxyMonitorCollector(ScheduledExecutorService scheduled,
                                        SimpleKeyedObjectPool<Endpoint, NettyClient> keyedObjectPool,
                                        ProxyModel model) {
        this.scheduled = scheduled;
        this.model = model;
        this.objectPool = keyedObjectPool.getKeyPool(new DefaultProxyEndpoint(model.getUri()));
    }

    @Override
    public ProxyModel getProxyInfo() {
        return model;
    }

    @Override
    public List<PingStatsResult> getPingStatsResults() {
        return pingStatsResults;
    }

    @Override
    public List<TunnelStatsResult> getTunnelStatsResults() {
        return tunnelStatsResults;
    }

    @Override
    public List<TunnelSocketStatsResult> getTunnelSocketStatsResults() {
        return socketStatsResults;
    }

    @Override
    public List<TunnelInfo> getTunnelInfos() {
        return tunnelInfos;
    }

    @Override
    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    @Override
    protected void doStart() {
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                new PingResultsUpdater().update();
                new TunnelStatsResultsUpdater().update();
                new SocketStatsResultsUpdater().update();
                new TunnelAggregator().update();
                notifyListeners();
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStop() {
        if(future != null) {
            future.cancel(true);
        }
    }

    private void notifyListeners() {
        listeners.forEach(listener -> listener.onChange(this));
    }

    private abstract class AbstractInfoUpdater<T> {

        public void update() {
            Command<T[]> command = getCommand();
            command.future().addListener(new CommandFutureListener<T[]>() {
                @Override
                public void operationComplete(CommandFuture<T[]> commandFuture) throws Exception {
                    if(!commandFuture.isSuccess()) {
                        logger.error("[update][{}]", getClass().getSimpleName(), commandFuture.cause());
                    }
                    updateRelevantField(commandFuture.getNow());
                }
            });
            command.execute();
        }

        protected abstract void updateRelevantField(T[] updates);

        protected abstract Command<T[]> getCommand();
    }

    private class PingResultsUpdater extends AbstractInfoUpdater<PingStatsResult> {

        @Override
        protected void updateRelevantField(PingStatsResult[] updates) {
            synchronized (this) {
                pingStatsResults = Lists.newArrayList(updates);
            }
        }

        @Override
        protected Command<PingStatsResult[]> getCommand() {
            return new AbstractProxyMonitorCommand.ProxyMonitorPingStatsCommand(objectPool, scheduled);
        }
    }

    private class SocketStatsResultsUpdater extends AbstractInfoUpdater<TunnelSocketStatsResult> {

        @Override
        protected void updateRelevantField(TunnelSocketStatsResult[] updates) {
            synchronized (this) {
                socketStatsResults = Lists.newArrayList(updates);
            }
        }

        @Override
        protected Command<TunnelSocketStatsResult[]> getCommand() {
            return new AbstractProxyMonitorCommand.ProxyMonitorSocketStatsCommand(objectPool, scheduled);
        }
    }

    private class TunnelStatsResultsUpdater extends AbstractInfoUpdater<TunnelStatsResult> {

        @Override
        protected void updateRelevantField(TunnelStatsResult[] updates) {
            synchronized (this) {
                tunnelStatsResults = Lists.newArrayList(updates);
            }
        }

        @Override
        protected Command<TunnelStatsResult[]> getCommand() {
            return new AbstractProxyMonitorCommand.ProxyMonitorTunnelStatsCommand(objectPool, scheduled);
        }
    }

    private class TunnelAggregator {

        public void update() {
            Map<String, DefaultTunnelInfo> tunnels = Maps.newHashMap();
            for(TunnelStatsResult tunnelStats : getTunnelStatsResults()) {
                String id = tunnelStats.getTunnelId();
                if(!tunnels.containsKey(id)) {
                    tunnels.put(id, new DefaultTunnelInfo(getProxyInfo(), id));
                }
                tunnels.get(id).setTunnelStatsResult(tunnelStats);
            }
            for(TunnelSocketStatsResult socketStats : getTunnelSocketStatsResults()) {
                String id = socketStats.getTunnelId();
                tunnels.get(id).setSocketStatsResult(socketStats);
            }
            tunnelInfos = Lists.newArrayList(tunnels.values());
        }
    }
}
