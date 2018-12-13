package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.redis.console.model.consoleportal.TunnelSocketStatsMetricOverview;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelSocketStatsResult;

import java.util.List;

public interface TunnelSocketStatsAnalyzerManager extends Startable, Stoppable {

    List<TunnelSocketStatsAnalyzer.FrontendAndBackendMetrics> analyze(ProxyChain chain);

    TunnelSocketStatsMetricOverview analyze(TunnelSocketStatsResult result);

    List<String> getMetricTypes();
}
