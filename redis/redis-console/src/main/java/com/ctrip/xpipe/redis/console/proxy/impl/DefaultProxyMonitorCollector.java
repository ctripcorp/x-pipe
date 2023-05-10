package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SafeLoop;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollector;
import com.ctrip.xpipe.redis.core.proxy.command.AbstractProxyMonitorCommand;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.monitor.PingStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelSocketStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelTrafficResult;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

public class DefaultProxyMonitorCollector extends AbstractStartStoppable implements ProxyMonitorCollector {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProxyMonitorCollector.class);

    private List<PingStatsResult> pingStatsResults;

    private List<TunnelStatsResult> tunnelStatsResults;

    private List<TunnelSocketStatsResult> socketStatsResults;

    private List<TunnelTrafficResult> tunnelTrafficResults;

    private List<Listener> listeners = Lists.newCopyOnWriteArrayList();

    @JsonIgnore
    private List<DefaultTunnelInfo> tunnelInfos;

    @JsonIgnore
    private ScheduledFuture future;

    @JsonIgnore
    private ScheduledExecutorService scheduled;

    @JsonIgnore
    private SimpleObjectPool<NettyClient> objectPool;

    private ProxyModel model;

    private IntSupplier checkInterval;

    private Random random = new Random();

    public DefaultProxyMonitorCollector(ScheduledExecutorService scheduled,
                                        SimpleKeyedObjectPool<Endpoint, NettyClient> keyedObjectPool,
                                        ProxyModel model, IntSupplier checkInterval) {
        this.scheduled = scheduled;
        this.model = model;
        this.objectPool = keyedObjectPool.getKeyPool(new DefaultProxyEndpoint(model.getUri()));
        this.checkInterval = checkInterval;
    }

    @Override
    public ProxyModel getProxyInfo() {
        return model;
    }

    @Override
    public List<PingStatsResult> getPingStatsResults() {
        return pingStatsResults == null ? Collections.emptyList() : pingStatsResults;
    }

    @Override
    public List<TunnelStatsResult> getTunnelStatsResults() {
        return tunnelStatsResults == null ? Collections.emptyList() : tunnelStatsResults;
    }

    @Override
    public List<TunnelSocketStatsResult> getTunnelSocketStatsResults() {
        return socketStatsResults == null ? Collections.emptyList() : socketStatsResults;
    }

    @Override
    public List<TunnelTrafficResult> getTunnelTrafficResults() {
        return tunnelTrafficResults == null ? Collections.emptyList() : tunnelTrafficResults;
    }

    @Override
    public List<DefaultTunnelInfo> getTunnelInfos() {
        return tunnelInfos == null ? Collections.emptyList() : tunnelInfos;
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
                logger.debug("[doStart] start to monitor proxy:{} every {} seconds", model.getUri(), checkInterval.getAsInt() / 1000);
                SequenceCommandChain serial = new SequenceCommandChain(
                        new PingResultsUpdater().command(),
                        new TunnelStatsResultsUpdater().command(),
                        new SocketStatsResultsUpdater().command(),
                        new TrafficStatsResultsUpdater().command(),
                        new TunnelAggregator().command());

                serial.execute().addListener(commandFuture -> {
                    if(!commandFuture.isSuccess()) {
                        logger.error("[doStart]", commandFuture.cause());
                    } else {
                        new SafeLoop<Listener>(listeners) {
                            @Override
                            protected void doRun0(Listener listener) {
                                listener.ackPingStatsResult(DefaultProxyMonitorCollector.this, pingStatsResults);
                                listener.ackTrafficStatsResult(DefaultProxyMonitorCollector.this, tunnelTrafficResults);
                            }
                        }.run();
                    }
                });
            }
        }, getStartInterval(), checkInterval.getAsInt(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStop() {
        if(future != null) {
            future.cancel(true);
        }
    }

    protected int getStartInterval() {
        return 2000 + Math.abs(random.nextInt(3000));
    }

    private abstract class AbstractInfoUpdater<T> {

        public Command<T[]> command() {
            Command<T[]> command = getCommand();
            command.future().addListener(new CommandFutureListener<T[]>() {
                @Override
                public void operationComplete(CommandFuture<T[]> commandFuture) {
                    if(!commandFuture.isSuccess()) {
                        logger.error("[update][{}]", getClass().getSimpleName(), commandFuture.cause());
                        return;
                    }
                    updateRelevantField(commandFuture.getNow());
                }
            });
            return command;
        }

        protected abstract void updateRelevantField(T[] updates);

        protected abstract Command<T[]> getCommand();
    }

    private class PingResultsUpdater extends AbstractInfoUpdater<PingStatsResult> {

        @Override
        protected void updateRelevantField(PingStatsResult[] updates) {
            synchronized (DefaultProxyMonitorCollector.this) {
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
            synchronized (DefaultProxyMonitorCollector.this) {
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
            synchronized (DefaultProxyMonitorCollector.this) {
                tunnelStatsResults = Lists.newArrayList(updates);
            }
        }

        @Override
        protected Command<TunnelStatsResult[]> getCommand() {
            return new AbstractProxyMonitorCommand.ProxyMonitorTunnelStatsCommand(objectPool, scheduled);
        }
    }

    private class TrafficStatsResultsUpdater extends AbstractInfoUpdater<TunnelTrafficResult> {

        @Override
        protected void updateRelevantField(TunnelTrafficResult[] updates) {
            synchronized (DefaultProxyMonitorCollector.this) {
                tunnelTrafficResults = Lists.newArrayList(updates);
            }
        }

        @Override
        protected Command<TunnelTrafficResult[]> getCommand() {
            return new AbstractProxyMonitorCommand.ProxyMonitorTrafficStatsCommand(objectPool, scheduled);
        }
    }

    private class TunnelAggregator {

        public Command<Void> command() {
            return new AbstractCommand<Void>() {
                @Override
                protected void doExecute() throws Exception {
                    update();
                    future().setSuccess();
                }

                @Override
                protected void doReset() {

                }

                @Override
                public String getName() {
                    return "TunnelAggregator";
                }
            };
        }

        private void update() {
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
                if(!tunnels.containsKey(id)) {
                    tunnels.put(id, new DefaultTunnelInfo(getProxyInfo(), id));
                }
                tunnels.get(id).setTunnelSocketStatsResult(socketStats);
            }
            for(TunnelTrafficResult trafficResult : getTunnelTrafficResults()) {
                String id = trafficResult.getTunnelId();
                if(!tunnels.containsKey(id)) {
                    tunnels.put(id, new DefaultTunnelInfo(getProxyInfo(), id));
                }
                tunnels.get(id).setTunnelTrafficResult(trafficResult);
            }
            tunnelInfos = Lists.newArrayList(tunnels.values());
            logger.debug("[TunnelAggregator] {}", tunnelInfos);
        }
    }
}
