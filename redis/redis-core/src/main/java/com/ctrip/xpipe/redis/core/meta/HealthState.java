package com.ctrip.xpipe.redis.core.meta;

/** Health of a keeper: {@code isUp()} gates a fresh election, {@code isElectable()} gates being active. */
public enum HealthState {

    HEALTHY(true, true),
    SICK(true, false),
    DOWN(false, false),
    RECOVERED_PENDING(true, false);

    private final boolean up;
    private final boolean electable;

    HealthState(boolean up, boolean electable) {
        this.up = up;
        this.electable = electable;
    }

    public boolean isUp() {
        return up;
    }

    public boolean isElectable() {
        return electable;
    }

    public static boolean isElectable(String healthState) {
        return parse(healthState).isElectable();
    }

    /** Null/blank (no node yet, or a legacy node) and unrecognized values are HEALTHY. */
    public static HealthState parse(String healthState) {
        if (healthState == null || healthState.trim().isEmpty()) {
            return HEALTHY;
        }
        try {
            return valueOf(healthState.trim());
        } catch (IllegalArgumentException e) {
            return HEALTHY;
        }
    }
}
