package com.ctrip.framework.xpipe.redis.servlet.spring;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class NoneFilterRegistrationCondition implements Condition {

    @Override
    public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {
        try {
            Class.forName("org.springframework.boot.context.embedded.FilterRegistrationBean");
            return false;
        } catch (Throwable e) {
            try {
                Class.forName("org.springframework.boot.web.servlet.FilterRegistrationBean");
                return true;
            } catch (Throwable e1) {
                return false;
            }
        }
    }
}
