package com.ctrip.xpipe.redis.keeper.config;

import java.util.Collections;
import java.util.List;

public class TestKeeperCommonConfig implements KeeperCommonConfig {

    @Override
    public List<KeeperReplDelayConfig> getReplDelayConfigs() {
        return Collections.emptyList();
    }
}
