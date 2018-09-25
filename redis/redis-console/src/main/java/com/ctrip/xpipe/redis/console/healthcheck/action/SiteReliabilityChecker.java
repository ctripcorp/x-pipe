package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.redis.console.healthcheck.action.event.AbstractInstanceEvent;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
public interface SiteReliabilityChecker {

    boolean isSiteHealthy(AbstractInstanceEvent event);
}
