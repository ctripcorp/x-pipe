package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.utils.LeakyBucket;

/**
 * @author chen.zhu
 * <p>
 * Feb 19, 2020
 */
public class DefaultKeeperResourceManager implements KeeperResourceManager {

    private LeakyBucket leakyBucket;

    public DefaultKeeperResourceManager(LeakyBucket leakyBucket) {
        this.leakyBucket = leakyBucket;
    }

    @Override
    public LeakyBucket getLeakyBucket() {
        return leakyBucket;
    }
}
