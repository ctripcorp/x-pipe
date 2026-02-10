package com.ctrip.xpipe.redis.keeper.config;

import java.util.List;
import java.util.Map;

public interface KeeperCommonConfig {

    List<KeeperReplDelayConfig> getKeeperReplDelayConfigs();

    Map<String, RedisReplDelayConfig> getRedisReplDelayConfigs();

    int getCrossRegionBytesLimit();

    int getCrossRegionMinBytesLimit();

    int getCrossRegionRateCheckInterval();

    int getCrossRegionRateIncreaseCheckRounds();

    int getCrossRegionRateDecreaseCheckRounds();

    boolean isCrossRegionRateLimitEnabled();

}
