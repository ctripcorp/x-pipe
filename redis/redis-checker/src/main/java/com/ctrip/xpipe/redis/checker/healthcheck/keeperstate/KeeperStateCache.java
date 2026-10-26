package com.ctrip.xpipe.redis.checker.healthcheck.keeperstate;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperstate.KeeperStateActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperstate.KeeperStateActionListener;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TFS Keeper 的实时 ACTIVE / BACKUP 状态。作为 {@link KeeperStateActionListener} 接收
 * {@code KeeperStateAction} 的 {@code KEEPER GETSTATE} 结果，供延迟打点查询。
 *
 * <p>只有 TFS Keeper 会进入本缓存：{@code KeeperStateActionController} 已卡住 {@code isTfs()}。因此读侧不能把
 * 「键不存在」解释为「不是 TFS」——它同样可能是开关关闭、首轮未回或命令失败。磁盘类型一律查 meta，
 * 本缓存只回答「这个 Keeper 现在是什么角色」。
 *
 * <p>条目会过期：Keeper 降级或失联、或采集开关关闭后不应继续按陈旧的 ACTIVE 标记判断，过期即视为未采到。
 *
 * <p>本缓存不自行扫描回收：action 停止时经 {@link #stopWatch} 删除；Keeper 不再被检查时，
 * {@code DefaultKeeperSessionManager} 回收其 session 的同时也会删除对应条目。
 */
@Component
public class KeeperStateCache implements KeeperStateActionListener {

    /** 两个采集周期（{@code keeper.state.check.interval.milli}）内没有新样本即视为失效。 */
    private static final int EXPIRE_INTERVALS = 2;

    /** 兜底窗口：配置为 0 或负数时若按原值计算，任何样本都会立刻过期，该 tag 将永久打不出来。 */
    private static final long MIN_EXPIRE_MILLI = 1000L;

    private final ConcurrentMap<HostPort, Sample> samples = new ConcurrentHashMap<>();

    private final CheckerConfig checkerConfig;

    @Autowired
    public KeeperStateCache(CheckerConfig checkerConfig) {
        this.checkerConfig = checkerConfig;
    }

    /**
     * 成功写入角色，失败写入「未知」样本而非直接删除：listener 是异步回调的，若失败时直接删，
     * 一个更早发出、却更晚到达的成功结果会把条目重新写回。
     */
    @Override
    public void onAction(KeeperStateActionContext context) {
        HostPort keeper = context.instance().getCheckInfo().getHostPort();
        KeeperState state = context.isSuccess() ? context.getResult() : null;
        record(keeper, state, context.getRequestTimeMilli());
    }

    @Override
    public void stopWatch(HealthCheckAction<KeeperHealthCheckInstance> action) {
        remove(action.getActionInstance().getCheckInfo().getHostPort());
    }

    @Override
    public boolean supportInstance(KeeperHealthCheckInstance instance) {
        return true;
    }

    /** 以当前时刻写入一个样本，供不经过 action 的调用方（如测试）使用。 */
    public void update(HostPort keeper, KeeperState state) {
        if (null == state) {
            return;
        }
        record(keeper, state, System.currentTimeMillis());
    }

    public void remove(HostPort keeper) {
        if (null != keeper) {
            samples.remove(keeper);
        }
    }

    /**
     * @return 未采到、最近一次采集失败或样本已过期时返回 null
     */
    public KeeperState getState(HostPort keeper) {
        if (null == keeper) {
            return null;
        }
        Sample sample = samples.get(keeper);
        if (null == sample || isExpired(sample)) {
            return null;
        }
        return sample.state;
    }

    /**
     * @return 未采到、最近一次采集失败或样本已过期时返回 null，调用方需与 {@code Boolean.FALSE} 区分
     */
    public Boolean isActive(HostPort keeper) {
        KeeperState state = getState(keeper);
        return null == state ? null : state.isActive();
    }

    /** 按请求发出时间取新者：晚发出的结果才能覆盖早发出的，与到达顺序无关。 */
    private void record(HostPort keeper, KeeperState state, long requestTimeMilli) {
        if (null == keeper) {
            return;
        }
        Sample sample = new Sample(state, requestTimeMilli);
        samples.merge(keeper, sample, (old, fresh) -> fresh.timeMilli >= old.timeMilli ? fresh : old);
    }

    private boolean isExpired(Sample sample) {
        long maxAge = Math.max(MIN_EXPIRE_MILLI,
                (long) EXPIRE_INTERVALS * checkerConfig.getKeeperStateCheckIntervalMilli());
        return System.currentTimeMillis() - sample.timeMilli >= maxAge;
    }

    private static final class Sample {

        /** null 表示最近一次采集失败 */
        private final KeeperState state;

        /** 请求发出时间 */
        private final long timeMilli;

        private Sample(KeeperState state, long timeMilli) {
            this.state = state;
            this.timeMilli = timeMilli;
        }
    }
}
