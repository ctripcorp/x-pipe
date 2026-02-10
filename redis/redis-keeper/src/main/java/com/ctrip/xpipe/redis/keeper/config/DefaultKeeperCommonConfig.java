package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.api.config.ConfigProvider;
import com.ctrip.xpipe.config.AbstractConfigBean;

import java.util.List;
import java.util.Map;

public class DefaultKeeperCommonConfig extends AbstractConfigBean implements KeeperCommonConfig {

    private static String KEY_KEEPER_REPL_DELAY_CONFIG = "keeper.repl.delay.config";
    private static String KEY_REDIS_REPL_DELAY_CONFIG = "redis.repl.delay.config";
    private static String KEY_CROSSREGION_RATE_LIMIT = "keeper.crossregion.rate.limit.bytes";
    private static String KEY_CROSSREGION_MIN_RATE_LIMIT = "keeper.crossregion.rate.limit.bytes.min";
    private static String KEY_CROSSREGION_RATE_CHECK_INTERVAL = "keeper.crossregion.rate.limit.check.interval";
    private static String KEY_CROSSREGION_RATE_INCREASE_ROUND = "keeper.crossregion.rate.limit.increase.round";
    private static String KEY_CROSSREGION_RATE_DECREASE_ROUND = "keeper.crossregion.rate.limit.decrease.round";
    private static String KEY_CROSSREGION_RATE_LIMIT_ENABLED = "keeper.crossregion.rate.limit.enabled";

    private final GenericTypeReference<List<KeeperReplDelayConfig>> keeperReplDelayConfigListType = new GenericTypeReference<List<KeeperReplDelayConfig>>() {};
    private final GenericTypeReference<Map<String, RedisReplDelayConfig>> redisReplDelayConfigMapType = new GenericTypeReference<Map<String, RedisReplDelayConfig>>() {};

    public DefaultKeeperCommonConfig(){
        super(ConfigProvider.DEFAULT.getOrCreateConfig(ConfigProvider.COMMON_CONFIG));
    }

    @Override
    public List<KeeperReplDelayConfig> getKeeperReplDelayConfigs() {
        String replDelayConfigInfo = getProperty(KEY_KEEPER_REPL_DELAY_CONFIG, "[]");
        return Codec.DEFAULT.decode(replDelayConfigInfo, keeperReplDelayConfigListType);
    }

    @Override
    public Map<String, RedisReplDelayConfig> getRedisReplDelayConfigs() {
        String replDelayConfigInfo = getProperty(KEY_REDIS_REPL_DELAY_CONFIG, "{}");
        return Codec.DEFAULT.decode(replDelayConfigInfo, redisReplDelayConfigMapType);
    }

    @Override
    public int getCrossRegionBytesLimit() {
        return getIntProperty(KEY_CROSSREGION_RATE_LIMIT, 50 * 1024 * 1024);
    }

    @Override
    public int getCrossRegionMinBytesLimit() {
        return getIntProperty(KEY_CROSSREGION_MIN_RATE_LIMIT, 1024 * 1024);
    }

    @Override
    public int getCrossRegionRateCheckInterval() {
        return getIntProperty(KEY_CROSSREGION_RATE_CHECK_INTERVAL, 30);
    }

    @Override
    public int getCrossRegionRateIncreaseCheckRounds() {
        return getIntProperty(KEY_CROSSREGION_RATE_INCREASE_ROUND, 1);
    }

    @Override
    public int getCrossRegionRateDecreaseCheckRounds() {
        return getIntProperty(KEY_CROSSREGION_RATE_DECREASE_ROUND, 10);
    }

    @Override
    public boolean isCrossRegionRateLimitEnabled() {
        return getBooleanProperty(KEY_CROSSREGION_RATE_LIMIT_ENABLED, true);
    }
}
