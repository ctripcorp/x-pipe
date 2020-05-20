package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.utils.LeakyBucket;

/**
 * @author chen.zhu
 * <p>
 * Feb 19, 2020
 */
public interface KeeperResourceManager extends ProxyResourceManager {
    LeakyBucket getLeakyBucket();
}
