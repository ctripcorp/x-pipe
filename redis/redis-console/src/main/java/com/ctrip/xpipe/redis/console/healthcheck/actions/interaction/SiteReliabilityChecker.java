package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction;

import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.AbstractInstanceEvent;

/**
 * @author chen.zhu
 * <p>
 * Sep 18, 2018
 */
public interface SiteReliabilityChecker {

    boolean isSiteHealthy(AbstractInstanceEvent event);
}
