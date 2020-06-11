package com.ctrip.xpipe.redis.console.controller.annotation;

import com.ctrip.xpipe.cluster.ClusterType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ClusterTypeLimit {

    ClusterType[] value() default { ClusterType.ONE_WAY };

    String clusterFieldName() default "clusterId";

}
