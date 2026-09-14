package com.ctrip.xpipe.redis.checker.healthcheck;

/**
 * Factory boundary for Keeper-only health-check actions. Implementations own
 * cleanup for every lifecycle phase, including a partially failed initialize.
 *
 * @param <T> Keeper action type
 */
public interface KeeperHealthCheckActionFactory<T extends HealthCheckAction<KeeperHealthCheckInstance>>
        extends HealthCheckActionFactory<T, KeeperHealthCheckInstance> {

    /**
     * Release all resources held by the action. Implementations must also work
     * when initialize or start failed partway through.
     */
    @Override
    void destroy(T action) throws Exception;
}
