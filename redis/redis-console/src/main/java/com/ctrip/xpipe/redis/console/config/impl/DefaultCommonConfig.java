package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.api.config.ConfigProvider;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import org.springframework.stereotype.Component;

import java.util.Set;

import static com.ctrip.xpipe.api.config.ConfigProvider.COMMON_CONFIG;

/**
 * @author lishanglin
 * date 2024/7/10
 */
@Component
public class DefaultCommonConfig extends AbstractConfigBean implements ConsoleCommonConfig {

    // beacon
    private static final String KEY_BEACON_SUPPORT_ZONE = "beacon.zone";
    private static final String KEY_MONITOR_UNREGISTER_PROTECT_COUNT = "monitor.unregister.protect.count";

    public DefaultCommonConfig() {
        super(ConfigProvider.DEFAULT.getOrCreateConfig(COMMON_CONFIG));
    }

    @Override
    public Set<String> getBeaconSupportZones() {
        return getSplitStringSet(getProperty(KEY_BEACON_SUPPORT_ZONE, ""));
    }

    @Override
    public int monitorUnregisterProtectCount() {
        return getIntProperty(KEY_MONITOR_UNREGISTER_PROTECT_COUNT, 10);
    }

}
