package com.ctrip.xpipe.redis.console.health.action;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
public enum HEALTH_STATE {

    UP(false, true),
    UNKNOWN(true, true),
    UNHEALTHY(false, true),
    DOWN(true, false);

    private boolean toUpNotify;
    private boolean toDownNotify;

    HEALTH_STATE(boolean toUpNotify, boolean toDownNotify){
        this.toUpNotify = toUpNotify;
        this.toDownNotify = toDownNotify;
    }

    public boolean isToDownNotify() {
        return toDownNotify;
    }

    public boolean isToUpNotify() {
        return toUpNotify;
    }
}
