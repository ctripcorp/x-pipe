package com.ctrip.xpipe.redis.checker.spring;

import com.ctrip.xpipe.redis.checker.config.impl.CommonConfigBean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.util.Map;

public class ConsoleDisableDbCondition implements Condition {

    private static CommonConfigBean commonConfigBean = new CommonConfigBean();


    @Override
    public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {
        Map<String, Object> attributes = annotatedTypeMetadata.getAnnotationAttributes(DisableDbMode.class.getName());
        boolean disable = (boolean) attributes.getOrDefault("value", false);
        return commonConfigBean.disableDb() == disable;
    }


}
