package com.ctrip.xpipe.redis.core.store;

/**
 * @author lishanglin
 * date 2021/7/21
 */
public interface CommandsGuarantee {

    long getNeededCommandOffset();

    boolean isFinish();

    boolean isTimeout();

}
