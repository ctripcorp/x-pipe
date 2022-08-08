package com.ctrip.xpipe.redis.checker.healthcheck.stability;

/**
 * @author lishanglin
 * date 2022/8/8
 */
public interface StabilityHolder {

    boolean isSiteStable();

    DefaultStabilityHolder.Desc getDebugDesc();

    void setStaticStable(boolean stable, int ttl);

    void useDynamicStable();

}
