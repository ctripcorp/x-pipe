package com.ctrip.xpipe.redis.checker.healthcheck.meta;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
public interface MetaVisitor<T> {

    void accept(T t);
}
