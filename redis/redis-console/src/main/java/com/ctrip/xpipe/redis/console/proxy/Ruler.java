package com.ctrip.xpipe.redis.console.proxy;

public interface Ruler<T> {

    boolean matches(T t);

}
