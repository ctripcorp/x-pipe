package com.ctrip.xpipe.redis.checker.spring;

import com.ctrip.xpipe.redis.checker.config.impl.CommonConfigBean;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.ConfigurationCondition;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class FiremanServletScanCondition implements ConfigurationCondition {

    private static final ConsoleServerModeCondition SERVER_MODE_CONDITION = new ConsoleServerModeCondition();

    private static final CommonConfigBean COMMON_CONFIG = new CommonConfigBean();

    @Override
    public ConfigurationPhase getConfigurationPhase() {
        return ConfigurationPhase.PARSE_CONFIGURATION;
    }

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        return shouldScan(SERVER_MODE_CONDITION.getMode(), COMMON_CONFIG.disableDb());
    }

    static boolean shouldScan(ConsoleServerModeCondition.SERVER_MODE mode, boolean disableDb) {
        return mode != ConsoleServerModeCondition.SERVER_MODE.CHECKER && !disableDb;
    }
}
