package com.ctrip.xpipe.redis.keeper.storage.health;

/** Origin of an instrumented IO task. */
public enum IoSource {
    BUSINESS,
    PROBE
}
