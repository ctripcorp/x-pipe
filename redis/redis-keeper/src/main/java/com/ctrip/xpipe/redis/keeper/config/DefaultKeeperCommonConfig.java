package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.api.config.ConfigProvider;
import com.ctrip.xpipe.config.CompositeConfig;
import com.ctrip.xpipe.config.DefaultPropertyConfig;
import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;

import java.util.List;
import java.util.Map;

public class DefaultKeeperCommonConfig extends AbstractCoreConfig implements KeeperCommonConfig {

    private static String KEY_KEEPER_REPL_DELAY_CONFIG = "keeper.repl.delay.config";
    private static String KEY_REDIS_REPL_DELAY_CONFIG = "redis.repl.delay.config";

    private final GenericTypeReference<List<KeeperReplDelayConfig>> keeperReplDelayConfigListType = new GenericTypeReference<List<KeeperReplDelayConfig>>() {};
    private final GenericTypeReference<Map<String, RedisReplDelayConfig>> redisReplDelayConfigMapType = new GenericTypeReference<Map<String, RedisReplDelayConfig>>() {};

    public DefaultKeeperCommonConfig(){
        CompositeConfig compositeConfig = new CompositeConfig();
        compositeConfig.addConfig(ConfigProvider.DEFAULT.getOrCreateConfig(ConfigProvider.COMMON_CONFIG));
        compositeConfig.addConfig(new DefaultPropertyConfig());
        setConfig(compositeConfig);
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
}
