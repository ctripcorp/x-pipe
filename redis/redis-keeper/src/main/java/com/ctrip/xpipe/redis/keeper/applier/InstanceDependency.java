package com.ctrip.xpipe.redis.keeper.applier;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 08:01
 *
 * See AbstractInstanceNode & AbstractInstanceComponent
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface InstanceDependency {
}