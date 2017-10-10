package com.ctrip.xpipe.redis.console.annotation;

import java.lang.annotation.*;



/**
 * @author shyin
 *
 * Aug 29, 2016
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DalTransaction {
	String value() default "";
}
