package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperstate;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.core.meta.KeeperState;

/**
 * 一次 {@code KEEPER GETSTATE} 的结果：成功时带 Keeper 的实时角色，失败时带 cause。
 *
 * <p>{@link #getRequestTimeMilli()} 是请求发出的时间。listener 由 executor 异步回调，到达顺序不保证与
 * 请求顺序一致，消费方应以它判断新旧 —— 不能用 {@code recvTimeMilli}，一次超时失败的响应时间会晚于
 * 下一轮更早返回的成功响应。
 */
public class KeeperStateActionContext extends AbstractActionContext<KeeperState, KeeperHealthCheckInstance> {

    private final long requestTimeMilli;

    public KeeperStateActionContext(KeeperHealthCheckInstance instance, KeeperState state, long requestTimeMilli) {
        super(instance, state);
        this.requestTimeMilli = requestTimeMilli;
    }

    public KeeperStateActionContext(KeeperHealthCheckInstance instance, Throwable cause, long requestTimeMilli) {
        super(instance, cause);
        this.requestTimeMilli = requestTimeMilli;
    }

    public long getRequestTimeMilli() {
        return requestTimeMilli;
    }
}
