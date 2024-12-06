package com.ctrip.xpipe.redis.keeper.config;

import java.util.List;

public interface KeeperCommonConfig {

    List<KeeperReplDelayConfig> getReplDelayConfigs();

}
