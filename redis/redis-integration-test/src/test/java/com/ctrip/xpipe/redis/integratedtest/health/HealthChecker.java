package com.ctrip.xpipe.redis.integratedtest.health;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

/**
 * @author chen.zhu
 * <p>
 * Jul 29, 2018
 */
public interface HealthChecker extends Startable, Stoppable, Releasable {

}
