package com.ctrip.xpipe.redis.meta.server.keeper.elect;

/**
 * Keeper active election strategy from QConfig {@code keeper.elect.strategy}.
 */
public enum KeeperElectStrategy {

    AUTO,
    BM_PREFER;

    public static KeeperElectStrategy from(String value) {
        if (value == null) {
            return AUTO;
        }
        try {
            return KeeperElectStrategy.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return AUTO;
        }
    }
}
