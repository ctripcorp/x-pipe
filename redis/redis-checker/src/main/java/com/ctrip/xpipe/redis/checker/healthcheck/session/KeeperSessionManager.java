package com.ctrip.xpipe.redis.checker.healthcheck.session;

/**
 * Session owner dedicated to Keeper health checks.
 *
 * Deliberately add-only, exactly like {@link RedisSessionManager}: sessions are reclaimed solely by
 * the periodic {@link AbstractInstanceSessionManager#removeUnusedInstances()} against the in-use set
 * of {@link DefaultKeeperSessionManager}. Callers must not close Keeper sessions on instance removal
 * -- a same-address Keeper modification is applied as remove-before-add, so an eager release would
 * only churn a connection that the immediately following add reuses.
 */
public interface KeeperSessionManager extends InstanceSessionManager {

}
