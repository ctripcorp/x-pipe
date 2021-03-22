package com.ctrip.xpipe.redis.checker.spring;

import org.springframework.context.annotation.Conditional;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author lishanglin
 * date 2021/3/13
 */
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Conditional(ConsoleServerModeCondition.class)
public @interface ConsoleServerMode {

    ConsoleServerModeCondition.SERVER_MODE value();

}
