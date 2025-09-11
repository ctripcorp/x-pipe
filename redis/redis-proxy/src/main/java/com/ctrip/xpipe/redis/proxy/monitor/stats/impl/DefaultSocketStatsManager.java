package com.ctrip.xpipe.redis.proxy.monitor.stats.impl;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.core.proxy.monitor.SocketStatsResult;
import com.ctrip.xpipe.redis.proxy.monitor.stats.SocketStatsManager;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.AbstractScriptExecutor;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


@Component
public class DefaultSocketStatsManager implements SocketStatsManager {

    protected Logger logger = LoggerFactory.getLogger(DefaultSocketStatsManager.class);

    private static final String SOCKET_STATS_SPLITTER = "\\s* \\s*";

    private static final String HOST_SPLITTER = "\\s*:\\s*";

    private static final String PROXY_SOCKET_STATS_COLLECT = "Proxy.Socket.State.Collect";

    private static final Map<LocalRemotePort, SocketStatsResult> EMPTY_MAP = Maps.newHashMap();

    private AtomicReference<Map<LocalRemotePort, SocketStatsResult>> allSocketStatsResult =  new AtomicReference<>(EMPTY_MAP);

    private ScheduledFuture future;

    @Autowired
    private ResourceManager resourceManager;

    public DefaultSocketStatsManager() {
    }

    @PostConstruct
    public void postConstruct() {
        ScheduledExecutorService scheduled = resourceManager.getGlobalSharedScheduled();
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                new SocketStatsScriptExecutor()
                        .execute()
                        .addListener(commandFuture -> {
                            if (!commandFuture.isSuccess()) {
                                EventMonitor.DEFAULT.logEvent(PROXY_SOCKET_STATS_COLLECT, commandFuture.cause().getMessage());
                            } else {
                                EventMonitor.DEFAULT.logEvent(PROXY_SOCKET_STATS_COLLECT, "success");
                                allSocketStatsResult.set(analyzeRawSocketStats(commandFuture.get()));
                            }
                        });
            }
        }, getCheckInterval(), getCheckInterval(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void preDestroy() {
        if (future != null) {
            future.cancel(true);
        }
    }

    @VisibleForTesting
    Map<LocalRemotePort, SocketStatsResult>  analyzeRawSocketStats(List<String> rawSocketStats) {
        Map<LocalRemotePort, SocketStatsResult> newSocketStats = Maps.newHashMap();
        int size = 0;
        if (rawSocketStats == null || (size = rawSocketStats.size()) == 0) {
            return newSocketStats;
        }

        if ((size & 1) != 0) {
            //if size is odd
            EventMonitor.DEFAULT.logAlertEvent("size of ss command result should be even");
            return newSocketStats;
        }

        for (int index = 0; index < size; index += 2) {
            LocalRemotePort localRemotePort = parseLocalRemotePort(rawSocketStats.get(index));
            if (null == localRemotePort) continue;

            newSocketStats.put(localRemotePort,
                new SocketStatsResult(Lists.newArrayList(rawSocketStats.get(index), rawSocketStats.get(index + 1))));
        }
        return newSocketStats;
    }

    private LocalRemotePort parseLocalRemotePort(String s) {
        String[] splits = s.split(SOCKET_STATS_SPLITTER);
        if (splits.length < 5) return null;
        try {
            int localPort = parsePort(splits[3]);
            int remotePort = parsePort(splits[4]);
            if (localPort == -1 || remotePort == -1) return null;
            return new LocalRemotePort(localPort, remotePort);
        } catch (Throwable th) {
            logger.error("[dealWithRawSocketStatsResult] parse local port or remote port fail", th);
            return null;
        }
    }

    private int parsePort(String host) {
        String[] result = host.split(HOST_SPLITTER);
        if (result.length < 5) return -1;
        return Integer.valueOf(result[4]);
    }

    private int getCheckInterval() {
        return resourceManager.getProxyConfig().socketStatsCheckInterval();
    }

    @Override
    public SocketStatsResult getSocketStatsResult(int localPort, int remotePort) {
        return allSocketStatsResult.get().get(new LocalRemotePort(localPort, remotePort));
    }

    protected static class LocalRemotePort extends Pair<Integer, Integer> {
        public LocalRemotePort(Integer key, Integer value) {
            super(key, value);
        }
    }

    static class SocketStatsScriptExecutor extends AbstractScriptExecutor<List<String>> {

        private static final String SS_TEMPLATE = "ss -itnm | grep -v State";

        private static Logger logger = LoggerFactory.getLogger(SocketStatsScriptExecutor.class);

        public SocketStatsScriptExecutor() {
        }

        @Override
        public String getScript() {
            return SS_TEMPLATE;
        }

        @Override
        public List<String> format(List<String> result) {
            return result;
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
