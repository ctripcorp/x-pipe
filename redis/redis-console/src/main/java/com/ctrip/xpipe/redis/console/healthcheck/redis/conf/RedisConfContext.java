package com.ctrip.xpipe.redis.console.healthcheck.redis.conf;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public interface RedisConfContext extends Lifecycle {

    String getVersion();

    String getXRedisVersion();

}
