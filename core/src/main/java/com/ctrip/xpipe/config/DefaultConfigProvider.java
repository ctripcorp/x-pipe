package com.ctrip.xpipe.config;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.config.ConfigProvider;
import com.ctrip.xpipe.utils.MapUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author lishanglin
 * date 2024/6/20
 */
public class DefaultConfigProvider implements ConfigProvider {

    private Map<String, Config> configs = new ConcurrentHashMap();

    @Override
    public Config getDefaultConfig() {
        return getOrCreateConfig(DefaultFileConfig.DEFAULT_CONFIG_FILE);
    }

    @Override
    public Config getOrCreateConfig(String name) {
        return MapUtils.getOrCreate(configs, name, () -> new DefaultFileConfig(name));
    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }
}
