package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

/**
 * @author ayq
 * <p>
 * 2022/8/22 15:39
 */
public interface RedisGtidCollector extends Releasable, Startable, Stoppable {
}
