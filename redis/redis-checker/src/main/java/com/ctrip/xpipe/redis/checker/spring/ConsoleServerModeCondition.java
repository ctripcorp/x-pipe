package com.ctrip.xpipe.redis.checker.spring;

import com.ctrip.xpipe.api.config.Config;
import com.google.common.base.Enums;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.util.Map;

/**
 * @author lishanglin
 * date 2021/3/13
 */
public class ConsoleServerModeCondition implements Condition {

    public static final String KEY_SERVER_MODE = "console.server.mode";

    public enum SERVER_MODE {
        CONSOLE_CHECKER,
        CONSOLE,
        CHECKER
    }

    @Override
    public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata metadata)
    {
        Map<String, Object> attributes = metadata.getAnnotationAttributes(ConsoleServerMode.class.getName());
        assert attributes != null;
        SERVER_MODE supportMode = (SERVER_MODE) attributes.get("value");
        return getMode().equals(supportMode);
    }

    protected SERVER_MODE getMode() {
        String value = Config.DEFAULT.get(KEY_SERVER_MODE, SERVER_MODE.CONSOLE_CHECKER.name());
        return Enums.getIfPresent(SERVER_MODE.class, value.toUpperCase()).or(SERVER_MODE.CONSOLE_CHECKER);
    }

}
