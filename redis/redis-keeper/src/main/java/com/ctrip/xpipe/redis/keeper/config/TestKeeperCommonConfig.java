package com.ctrip.xpipe.redis.keeper.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestKeeperCommonConfig implements KeeperCommonConfig {

    @Override
    public List<KeeperReplDelayConfig> getKeeperReplDelayConfigs() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, RedisReplDelayConfig> getRedisReplDelayConfigs() {
        return Collections.emptyMap();
    }

    @Override
    public int getCrossRegionBytesLimit() {
        return -1;
    }
}
