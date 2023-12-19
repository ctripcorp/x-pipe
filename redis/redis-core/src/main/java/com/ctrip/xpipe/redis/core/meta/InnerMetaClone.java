package com.ctrip.xpipe.redis.core.meta;

/**
 * @author lishanglin
 * date 2023/12/13
 */
public interface InnerMetaClone<T> {

    T clone(T o);

}
