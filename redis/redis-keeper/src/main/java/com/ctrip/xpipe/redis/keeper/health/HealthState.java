package com.ctrip.xpipe.redis.keeper.health;

/**
 * @author lishanglin
 * date 2023/12/9
 */
public enum HealthState {
    HEALTHY(true),
    SICK(true),
    DOWN(false);

    private boolean up;

    HealthState(boolean up) {
        this.up = up;
    }

    public boolean isUp() {
        return up;
    }
}
