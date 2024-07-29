package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.api.config.ConfigChangeListener;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SafeLoop;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.console.config.ConsoleConfigListener;
import com.ctrip.xpipe.redis.console.model.consoleportal.TunnelSocketStatsMetric;
import com.ctrip.xpipe.redis.console.model.consoleportal.TunnelSocketStatsMetricOverview;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainAnalyzer;
import com.ctrip.xpipe.redis.console.proxy.TunnelSocketStatsAnalyzer;
import com.ctrip.xpipe.redis.console.proxy.TunnelSocketStatsAnalyzerManager;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelSocketStatsResult;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ServicesUtil;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

@Component
public class DefaultTunnelSocketStatsAnalyzerManager extends AbstractStartStoppable implements TunnelSocketStatsAnalyzerManager {

    @Autowired
    private ConsoleConfig config;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executors;

    @Autowired
    private ProxyChainAnalyzer chainAnalyzer;

    private MetricProxy metricProxy = ServicesUtil.getMetricProxy();

    private Map<String, String> configuredAnalyzers = Maps.newConcurrentMap();

    private Map<String, TunnelSocketStatsAnalyzer> analyzers = Maps.newConcurrentMap();

    private List<String> stableAnalyzerTypes = Lists.newArrayList(RetransAnalyzer.KEY_RETRANS, RttAnalyzer.KEY_RTT,
            SendQueueAnalyzer.KEY_SEND_QUEUE, RecvQueueAnalyzer.KEY_RECV_QUEUE, SendRateAnalyzer.KEY_SEND_RATE);


    @PostConstruct
    public void postConstruct() {
        try {
            start();
        } catch (Exception e) {
            logger.error("[postConstruct]", e);
        }

        chainAnalyzer.addListener(new ProxyChainAnalyzer.Listener() {
            @Override
            public void onChange(Map<DcClusterShardPeer, ProxyChain> previous, Map<DcClusterShardPeer, ProxyChain> current) {
                new SafeLoop<ProxyChain>(executors, current.values()) {
                    @Override
                    protected void doRun0(ProxyChain chain) {
                        analyzeAndReport(chain);
                    }
                }.run();
            }
        });
    }

    private void analyzeAndReport(ProxyChain chain) {
        List<TunnelSocketStatsAnalyzer.FrontendAndBackendMetrics> metrics = analyze(chain);
        reportMetric(metrics);
    }

    private void reportMetric(List<TunnelSocketStatsAnalyzer.FrontendAndBackendMetrics> metrics) {
        if(metrics == null || metrics.isEmpty()) {
            return;
        }
        for(TunnelSocketStatsAnalyzer.FrontendAndBackendMetrics metric : metrics) {
            try {
                metricProxy.writeBinMultiDataPoint(metric.getFrontend());
                metricProxy.writeBinMultiDataPoint(metric.getBackend());
            } catch (Exception e) {
                logger.error("[reportMetric]", e);
            }
        }
    }

    @Override
    protected void doStart() {
        putAnalyzer(new SendQueueAnalyzer());
        putAnalyzer(new RecvQueueAnalyzer());
        putAnalyzer(new RetransAnalyzer());
        putAnalyzer(new RttAnalyzer());
        putAnalyzer(new SendRateAnalyzer());
    }

    @Override
    protected void doStop() {

    }

    @Override
    public List<TunnelSocketStatsAnalyzer.FrontendAndBackendMetrics> analyze(ProxyChain chain) {
        List<TunnelSocketStatsAnalyzer.FrontendAndBackendMetrics> result = Lists.newArrayList();
        new SafeLoop<TunnelSocketStatsAnalyzer>(analyzers.values()) {
            @Override
            protected void doRun0(TunnelSocketStatsAnalyzer analyzer) {
                result.addAll(analyzer.analyze(chain));
            }

        }.run();
        return result;
    }

    @Override
    public TunnelSocketStatsMetricOverview analyze(TunnelSocketStatsResult result) {
        List<Pair<TunnelSocketStatsMetric, TunnelSocketStatsMetric>> metrics = Lists.newArrayList();
        new SafeLoop<TunnelSocketStatsAnalyzer>(analyzers.values()) {
            @Override
            protected void doRun0(TunnelSocketStatsAnalyzer analyzer) {
                metrics.add(analyzer.analyze(result));
            }

        }.run();
        List<TunnelSocketStatsMetric> frontendMetric = Lists.newArrayListWithCapacity(metrics.size());
        List<TunnelSocketStatsMetric> backendMetric = Lists.newArrayListWithCapacity(metrics.size());
        for(Pair<TunnelSocketStatsMetric, TunnelSocketStatsMetric> frontAndBackendMetric : metrics) {
            frontendMetric.add(frontAndBackendMetric.getKey());
            backendMetric.add(frontAndBackendMetric.getValue());
        }
        return new TunnelSocketStatsMetricOverview(frontendMetric, backendMetric);
    }

    @Override
    public List<String> getMetricTypes() {
        return Lists.newArrayList(analyzers.keySet());
    }

    private void putAnalyzer(TunnelSocketStatsAnalyzer analyzer) {
        analyzers.put(analyzer.getType(), analyzer);
    }

    private void removeUsed() {
        new SafeLoop<TunnelSocketStatsAnalyzer>(analyzers.values()) {
            @Override
            protected void doRun0(TunnelSocketStatsAnalyzer analyzer) {
                if(!containsKey(analyzer.getType(), stableAnalyzerTypes)
                        && !configuredAnalyzers.containsValue(analyzer.getType())) {
                    analyzers.remove(analyzer.getType());
                }
            }
        }.run();

    }

    private boolean containsKey(String key, List<String> keys) {
        return keys.contains(key);
    }


    protected static class RetransAnalyzer extends AbstractMultiValueTunnelSocketStatsAnalyzer {

        protected static final String KEY_RETRANS = "retrans";

        public RetransAnalyzer() {
            super(KEY_RETRANS);
        }

        @Override
        protected double getValueFromString(String[] values) {
            return parseDouble(values[1]);
        }

        @Override
        public String getType() {
            return KEY_RETRANS;
        }
    }

    protected static class RttAnalyzer extends AbstractMultiValueTunnelSocketStatsAnalyzer {

        protected static final String KEY_RTT = "rtt";

        public RttAnalyzer() {
            super(KEY_RTT);
        }

        @Override
        protected double getValueFromString(String[] values) {
            return parseDouble(values[0]);
        }

        @Override
        public String getType() {
            return KEY_RTT;
        }
    }

    protected static abstract class AbstractKeyWordBaseLineAnalyzer extends AbstractTunnelSocketStatsAnalyzer {

        @Override
        protected double analyze(List<String> socketStats) {
            for(String line : socketStats) {
                if(!line.contains(keyWord())) {
                    continue;
                }
                String[] values = StringUtil.splitRemoveEmpty(getSplitter(), line);
                return getResultFromArray(values);
            }
            return 0;
        }

        protected abstract String getSplitter();

        protected abstract String keyWord();

        protected abstract double getResultFromArray(String[] values);
    }

    protected static class SendQueueAnalyzer extends AbstractKeyWordBaseLineAnalyzer {

        protected static final String KEY_SEND_QUEUE = "send.queue";

        protected static final String WHITE_SPACE = " ";

        protected static final String KEY_WORD = "ESTAB";

        @Override
        protected String getSplitter() {
            return WHITE_SPACE;
        }

        @Override
        protected String keyWord() {
            return KEY_WORD;
        }

        @Override
        protected double getResultFromArray(String[] values) {
            return Long.parseLong(values[2]);
        }

        @Override
        public String getType() {
            return KEY_SEND_QUEUE;
        }
    }

    protected static class RecvQueueAnalyzer extends AbstractKeyWordBaseLineAnalyzer {

        protected static final String KEY_RECV_QUEUE = "recv.queue";
        @Override
        protected String getSplitter() {
            return SendQueueAnalyzer.WHITE_SPACE;
        }

        @Override
        protected String keyWord() {
            return SendQueueAnalyzer.KEY_WORD;
        }

        @Override
        protected double getResultFromArray(String[] values) {
            return Long.parseLong(values[1]);
        }


        @Override
        public String getType() {
            return KEY_RECV_QUEUE;
        }
    }

    protected static class SendRateAnalyzer extends AbstractNormalKeyValueTunnelSocketStatsAnalyzer {

        private static final String KEY_SEND = "send";

        private static final String KEY_SEND_RATE = "send.rate";

        public SendRateAnalyzer() {
            super(KEY_SEND);
        }

        @Override
        protected double getValue(String value) {
            double number = parseDouble(value);
            int multiple = getMultiple(value);
            return number * multiple;
        }

        private int getMultiple(String value) {
            if(value.contains("K")) {
                return 1000;
            } else if(value.contains("M")) {
                return 1000 * 1000;
            }
            return 1;
        }

        @Override
        public String getType() {
            return KEY_SEND_RATE;
        }
    }

    protected static class ConfiguredAnalyzer extends AbstractNormalKeyValueTunnelSocketStatsAnalyzer {

        private String type;

        public ConfiguredAnalyzer(String key, String value) {
            super(key);
            type = value;
        }

        @Override
        public String getType() {
            return type;
        }
    }
}
