package com.ctrip.xpipe.redis.checker.healthcheck.keeperstate;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TFS Keeper 的实时 ACTIVE / BACKUP 状态，由 {@code KeeperDelayAction} 的 INFO replication 采集后写入。
 *
 * <p>只有 TFS Keeper 会进入本缓存：写入发生在 {@code doTask} 的 INFO 回调里，而该方法的门禁已经卡住
 * {@code isTfs()} 与 capability，非 TFS Keeper 到不了这一步。因此读侧不能把「键不存在」解释为
 * 「不是 TFS」——它同样可能是开关关闭、capability 未就绪、首轮未回或 INFO 失败。磁盘类型一律查 meta，
 * 本缓存只回答「这个 Keeper 现在是不是 ACTIVE」。
 *
 * <p>条目会过期：Keeper 降级或失联后不应继续按陈旧的 ACTIVE 标记判断，过期即视为未采到。
 */
@Component
public class KeeperStateCache {

    /** 与 {@code DefaultPingService} 同口径：两个检查周期内没有新样本即视为失效。 */
    private static final int EXPIRE_INTERVALS = 2;

    /** 兜底窗口：配置为 0 或负数时若按原值计算，任何样本都会立刻过期，该 tag 将永久打不出来。 */
    private static final long MIN_EXPIRE_MILLI = 1000L;

    private final ConcurrentMap<HostPort, Sample> samples = new ConcurrentHashMap<>();

    private final CheckerConfig checkerConfig;

    @Autowired
    public KeeperStateCache(CheckerConfig checkerConfig) {
        this.checkerConfig = checkerConfig;
    }

    public void update(HostPort keeper, KeeperState state) {
        if (null == keeper || null == state) {
            return;
        }
        samples.put(keeper, new Sample(state, System.currentTimeMillis()));
    }

    public void remove(HostPort keeper) {
        if (null != keeper) {
            samples.remove(keeper);
        }
    }

    /**
     * @return 未采到或样本已过期时返回 null，调用方需与 {@code Boolean.FALSE} 区分
     */
    public Boolean isActive(HostPort keeper) {
        if (null == keeper) {
            return null;
        }
        Sample sample = samples.get(keeper);
        if (null == sample || isExpired(sample)) {
            return null;
        }
        return sample.state.isActive();
    }

    private boolean isExpired(Sample sample) {
        long maxAge = Math.max(MIN_EXPIRE_MILLI,
                (long) EXPIRE_INTERVALS * checkerConfig.getRedisReplicationHealthCheckInterval());
        return System.currentTimeMillis() - sample.recvTimeMilli >= maxAge;
    }

    private static final class Sample {

        private final KeeperState state;

        private final long recvTimeMilli;

        private Sample(KeeperState state, long recvTimeMilli) {
            this.state = state;
            this.recvTimeMilli = recvTimeMilli;
        }
    }
}
