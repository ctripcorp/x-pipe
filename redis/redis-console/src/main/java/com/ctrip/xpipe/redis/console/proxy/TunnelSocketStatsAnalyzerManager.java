package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

import java.util.List;

public interface TunnelSocketStatsAnalyzerManager extends Startable, Stoppable {

    List<TunnelSocketStatsAnalyzer.FrontendAndBackendMetrics> analyze(ProxyChain chain);

}
