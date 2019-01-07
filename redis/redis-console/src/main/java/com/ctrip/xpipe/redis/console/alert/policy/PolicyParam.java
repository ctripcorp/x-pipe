package com.ctrip.xpipe.redis.console.alert.policy;


/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public interface PolicyParam<T, V> {

    T param(V v);

    boolean supports(Class<? extends PolicyParam> clazz);
}
