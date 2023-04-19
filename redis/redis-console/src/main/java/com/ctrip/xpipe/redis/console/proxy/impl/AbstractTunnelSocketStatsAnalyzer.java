package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.TunnelSocketStatsMetric;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;
import com.ctrip.xpipe.redis.console.proxy.TunnelSocketStatsAnalyzer;
import com.ctrip.xpipe.redis.core.proxy.exception.XPipeProxyResultException;
import com.ctrip.xpipe.redis.core.proxy.monitor.SocketStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelSocketStatsResult;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public abstract class AbstractTunnelSocketStatsAnalyzer implements TunnelSocketStatsAnalyzer {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private static final int THOUSAND = 1000;

    private static final String SOCKET_STATS_SPLITTER = "\\s* \\s*";

    private static final String HOST_SPLITTER = "\\s*:\\s*";

    private static final List<FrontendAndBackendMetrics> EMPTY_METRICS = Collections.EMPTY_LIST;

    @Override
    public List<FrontendAndBackendMetrics> analyze(ProxyChain chain) {
        List<TunnelInfo> tunnelInfos = chain.getTunnels();
        if(tunnelInfos == null || tunnelInfos.isEmpty()) {
            logger.warn("[analyze] chain contains no tunnel info; cluster: {}, shard: {}", chain.getCluster(), chain.getShard());
            return EMPTY_METRICS;
        }
        List<FrontendAndBackendMetrics> result = Lists.newArrayList();
        String clusterId = chain.getCluster(), shardId = chain.getShard();
        for(TunnelInfo info : tunnelInfos) {
            logger.debug("[analyze each info] {}", info);
            try {
                result.add(getMetrics(info, clusterId, shardId));
            } catch (Throwable th) {
//                ignore
            }
        }
        return result;
    }

    @Override
    public Pair<TunnelSocketStatsMetric, TunnelSocketStatsMetric> analyze(TunnelSocketStatsResult result) {
        Pair<TunnelSocketStatsMetric, TunnelSocketStatsMetric> frontendAndBackend = new Pair<>();
        frontendAndBackend.setKey(new TunnelSocketStatsMetric(getType(), analyze(result.getFrontendSocketStats().getResult())));
        frontendAndBackend.setValue(new TunnelSocketStatsMetric(getType(), analyze(result.getBackendSocketStats().getResult())));
        return frontendAndBackend;
    }

    @Override
    public void reportMetrics(List<FrontendAndBackendMetrics> metrics) {

    }

    private FrontendAndBackendMetrics getMetrics(TunnelInfo info, String clusterId, String shardId) {
        MetricData frontendMetric = getMetricTemplate(info, clusterId, shardId);
        MetricData backendMetric = getMetricTemplate(info, clusterId, shardId);
        return new FrontendAndBackendMetrics(getFrontendMetric(frontendMetric, info), getBackendMetric(backendMetric, info));
    }

    private MetricData getBackendMetric(MetricData metric, TunnelInfo info) {
        TunnelSocketStatsResult tunnelSocketStatsResult = info.getTunnelSocketStatsResult();
        if (tunnelSocketStatsResult == null) {
            logger.warn("[getBackendMetric]no tunnelSocketStatsResult found in tunnelInfo {}:{}", info.getTunnelDcId(), info.getTunnelId());
            throw new XPipeProxyResultException("no tunnelSocketStatsResult found in tunnelInfo");
        }

        SocketStatsResult socketStatsResult = tunnelSocketStatsResult.getBackendSocketStats();
        metric.setTimestampMilli(socketStatsResult.getTimestamp());
        metric.setValue(analyze(socketStatsResult.getResult()));

        TunnelStatsResult tunnelStatsResult = info.getTunnelStatsResult();
        if (tunnelStatsResult == null) {
            logger.warn("[getBackendMetric]no tunnelStatsResult found in tunnelInfo {}:{}", info.getTunnelDcId(), info.getTunnelId());
            throw new XPipeProxyResultException("no tunnelStatsResult found in tunnelInfo");
        }
        setSrcDstHostPorts(metric, socketStatsResult, false);
        return metric;
    }


    private MetricData getFrontendMetric(MetricData metric, TunnelInfo info) {
        TunnelSocketStatsResult tunnelSocketStatsResult = info.getTunnelSocketStatsResult();
        if (tunnelSocketStatsResult == null) {
            logger.warn("[getFrontendMetric]no tunnelSocketStatsResult found in tunnelInfo {}:{}", info.getTunnelDcId(), info.getTunnelId());
            throw new XPipeProxyResultException("no tunnelSocketStatsResult found in tunnelInfo");
        }

        SocketStatsResult socketStatsResult = tunnelSocketStatsResult.getFrontendSocketStats();
        metric.setTimestampMilli(socketStatsResult.getTimestamp());
        metric.setValue(analyze(socketStatsResult.getResult()));

        TunnelStatsResult tunnelStatsResult = info.getTunnelStatsResult();
        if (tunnelStatsResult == null) {
            logger.warn("[getFrontendMetric]no tunnelStatsResult found in tunnelInfo {}:{}", info.getTunnelDcId(), info.getTunnelId());
            throw new XPipeProxyResultException("no tunnelStatsResult found in tunnelInfo");
        }
        setSrcDstHostPorts(metric, socketStatsResult, true);
        return metric;
    }
    private void setSrcDstHostPorts(MetricData metric, SocketStatsResult socketStatsResult, boolean isFrontend) {
        metric.addTag("socketType", isFrontend ? "frontend" : "backend");
        String[] splits = socketStatsResult.getResult().get(0).split(SOCKET_STATS_SPLITTER);
        if (splits.length < 5) return;

        String[] localSplits = splits[3].split(HOST_SPLITTER);
        setHostPortTag(metric, localSplits[3], Integer.valueOf(localSplits[4]), isFrontend ? "src" : "dst");

        String[] remoteSplits = splits[4].split(HOST_SPLITTER);
        setHostPortTag(metric, remoteSplits[3], Integer.valueOf(remoteSplits[4]), isFrontend ? "dst" : "src");
    }

    private void setHostPortTag(MetricData metric, String host, int port, String prefix) {
        metric.addTag(prefix + "HostPort", new HostPort(host, port % THOUSAND).toString());
        metric.addTag(prefix + "ThousandfoldPort", String.valueOf(port / THOUSAND));
    }

    private MetricData getMetricTemplate(TunnelInfo info, String clusterId, String shardId) {
        ProxyModel proxyModel = info.getProxyModel();
        return new MetricData(getType(), proxyModel.getDcName(), clusterId, shardId);
    }

    protected abstract double analyze(List<String> socketStats);

}
