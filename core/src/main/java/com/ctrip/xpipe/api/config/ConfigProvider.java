package com.ctrip.xpipe.api.config;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.utils.ServicesUtil;

/**
 * @author lishanglin
 * date 2024/6/20
 */
public interface ConfigProvider extends Ordered {

    ConfigProvider DEFAULT = ServicesUtil.getConfigProviderService();

    String COMMON_CONFIG = "common.properties";

    String CHECK_CONFIG_NAME = "checker.properties";

    String CONSOLE_CONFIG_NAME = "console.properties";

    String DATA_CENTER_CONFIG_NAME = "dc.properties";

    Config getDefaultConfig();

    Config getOrCreateConfig(String name);

}
