package com.ctrip.xpipe.service.fireman;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.api.config.ConfigProvider;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author lishanglin
 * date 2021/11/24
 */
public class FiremanConfig extends AbstractConfigBean {

    public static final String KEY_FIREMAN_POOLS = "fireman.poolids";

    public static final String KEY_META_SYNC_EXTERNAL_DC = "meta.sync.external.dc";

    public FiremanConfig() {
        super(ConfigProvider.DEFAULT.getOrCreateConfig(ConfigProvider.COMMON_CONFIG));
    }

    public Set<String> getFiremanRelatedPools() {
        return getSplitStringSet(getProperty(KEY_FIREMAN_POOLS, ""));
    }

    public boolean disableDb() {
        return getSplitStringSet(getProperty(KEY_META_SYNC_EXTERNAL_DC, "")).stream()
                .map(String::toUpperCase)
                .collect(Collectors.toSet())
                .contains(FoundationService.DEFAULT.getDataCenter());
    }

}
