package com.ctrip.xpipe.service.config;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.config.ConfigProvider;
import com.ctrip.xpipe.utils.MapUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.ctrip.xpipe.service.config.QConfig.DEFAULT_XPIPE_CONFIG_NAME;

/**
 * @author lishanglin
 * date 2024/6/20
 */
public class QConfigProvider implements ConfigProvider {

    private Map<String, QConfig> configs = new ConcurrentHashMap<>();


    @Override
    public Config getDefaultConfig() {
        return getOrCreateConfig(DEFAULT_XPIPE_CONFIG_NAME);
    }

    @Override
    public Config getOrCreateConfig(String name) {
        return MapUtils.getOrCreate(configs, name, () -> new QConfig(name));
    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }
}
