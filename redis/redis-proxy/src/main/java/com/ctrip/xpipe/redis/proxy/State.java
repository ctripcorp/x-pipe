package com.ctrip.xpipe.redis.proxy;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public interface State<C extends State> {

    C nextAfterSuccess();

    C nextAfterFail();

    String name();

    boolean isValidNext(C c);
}
