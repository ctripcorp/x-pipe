package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperstate;

import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 按检查周期以 {@code KEEPER GETSTATE} 采集 Keeper 的实时角色，结果以 {@link KeeperStateActionContext}
 * 通知 {@link KeeperStateActionListener}（如 {@code KeeperStateCache}）。
 *
 * <p>不用 {@code INFO replication}：那条命令每次回整段 replication 文本，按周期逐 Keeper 发送时
 * 解析与分配开销都显著；本命令的响应只是一个状态字符串。
 *
 * <p>与延迟检测解耦：不依赖 pub/sub capability，也不受延迟开关影响，只受
 * {@code keeper.state.check.enabled} 控制。开关关闭后只是不再采集，已有结果由消费方按过期处理。
 */
public class KeeperStateAction extends AbstractHealthCheckAction<KeeperHealthCheckInstance> {

    private static final Logger logger = LoggerFactory.getLogger(KeeperStateAction.class);

    /** 误配为 0 或负数时兜底，避免对 Keeper 高频刷命令（scheduleWithFixedDelay 也不接受非正周期） */
    static final int MIN_CHECK_INTERVAL_MILLI = 1000;

    private final CheckerConfig checkerConfig;

    public KeeperStateAction(ScheduledExecutorService scheduled, KeeperHealthCheckInstance instance,
                             ExecutorService executors, CheckerConfig checkerConfig) {
        super(scheduled, instance, executors);
        this.checkerConfig = checkerConfig;
    }

    @Override
    protected void doTask() {
        if (!checkerConfig.isKeeperStateCheckEnabled()) {
            return;
        }
        long requestTimeMilli = System.currentTimeMillis();
        instance.getRedisSession().keeperGetState(new Callbackable<KeeperState>() {
            @Override
            public void success(KeeperState state) {
                notifyIfStarted(new KeeperStateActionContext(instance, state, requestTimeMilli));
            }

            @Override
            public void fail(Throwable throwable) {
                logger.debug("[doTask][{}] keeper getstate failed", instance.getCheckInfo().getHostPort(), throwable);
                // 命令有超时兜底，这个回调一定会到；问不通也要通知，消费方据此撤掉旧角色
                notifyIfStarted(new KeeperStateActionContext(instance, throwable, requestTimeMilli));
            }
        });
    }

    /** 停止后的在途响应不再通知：stopWatch 已清理过消费方的状态 */
    private void notifyIfStarted(KeeperStateActionContext context) {
        if (getLifecycleState().isStarted()) {
            notifyListeners(context);
        }
    }

    /**
     * 与 {@code RedisMasterCheckAction} 同配置：{@code redis.conf.check.interval}；{@code KeeperStateCache}
     * 的过期窗口按同一配置计算。与其他 action 一样只在启动时读取，修改后随实例重建生效。
     */
    @Override
    protected int getBaseCheckInterval() {
        return Math.max(MIN_CHECK_INTERVAL_MILLI, instance.getHealthCheckConfig().getRedisConfCheckIntervalMilli());
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }
}
