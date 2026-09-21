package com.ctrip.xpipe.service.fireman;

import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.api.config.ConfigProvider;

import java.util.Set;

/**
 * @author lishanglin
 * date 2021/11/24
 */
public class FiremanConfig extends AbstractConfigBean {

    public static final String KEY_FIREMAN_POOLS = "fireman.poolids";

    public FiremanConfig() {
        super(ConfigProvider.DEFAULT.getOrCreateConfig(ConfigProvider.COMMON_CONFIG));
    }

    public Set<String> getFiremanRelatedPools() {
        return getSplitStringSet(getProperty(KEY_FIREMAN_POOLS, ""));
    }

}
