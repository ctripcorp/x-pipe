package com.ctrip.xpipe.redis.core.proxy.endpoint;

/**
 * @author chen.zhu
 * <p>
 * May 31, 2018
 */
public interface SelectStrategy {

    boolean select();
}
