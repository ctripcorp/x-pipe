package com.ctrip.xpipe.redis.checker.healthcheck.keeperstate;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperstate.KeeperStateActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperstate.KeeperStateActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.meta.KeeperCheckSelector;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

import static com.ctrip.xpipe.redis.checker.resource.Resource.REDIS_COMMAND_EXECUTOR;

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
 * <p>条目回收：action 停止时经 {@link #stopWatch} 删除；另有定时任务按 {@link KeeperCheckSelector}
 * 的在用集合清掉已不在检查范围内的 Keeper 与过期样本，兜住 action 未能正常停止、或停止后仍有在途通知写回的情况。
 */
@Component
public class KeeperStateCache implements KeeperStateActionListener {

    private static final Logger logger = LoggerFactory.getLogger(KeeperStateCache.class);

    /** 两个采集周期（{@code keeper.state.check.interval.milli}）内没有新样本即视为失效。 */
    private static final int EXPIRE_INTERVALS = 2;

    /** 兜底窗口：配置为 0 或负数时若按原值计算，任何样本都会立刻过期，该 tag 将永久打不出来。 */
    private static final long MIN_EXPIRE_MILLI = 1000L;

    @VisibleForTesting
    static long cleanUpIntervalSeconds = 30;

    private final ConcurrentMap<HostPort, Sample> samples = new ConcurrentHashMap<>();

    private final CheckerConfig checkerConfig;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private KeeperCheckSelector keeperSelector;

    @Resource(name = REDIS_COMMAND_EXECUTOR)
    private ScheduledExecutorService scheduled;

    private ScheduledFuture<?> future;

    @Autowired
    public KeeperStateCache(CheckerConfig checkerConfig) {
        this.checkerConfig = checkerConfig;
    }

    @PostConstruct
    public void postConstruct() {
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                removeUnusedKeepers();
            }
        }, cleanUpIntervalSeconds, cleanUpIntervalSeconds, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void preDestroy() {
        if (null != future) {
            future.cancel(true);
        }
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

    /**
     * 清掉已不在检查范围内的 Keeper 与过期样本。读侧本就把过期样本当作未采到，这里只回收内存；
     * meta 不可用时只按过期清理，不据此判定「无 Keeper 在用」。
     */
    @VisibleForTesting
    void removeUnusedKeepers() {
        if (samples.isEmpty()) {
            return;
        }
        Set<HostPort> inUse = getInUseKeepers();
        for (Map.Entry<HostPort, Sample> entry : samples.entrySet()) {
            HostPort keeper = entry.getKey();
            Sample sample = entry.getValue();
            boolean unused = null != inUse && !inUse.contains(keeper);
            if (unused || isExpired(sample)) {
                // 条件删除：判定之后若有新样本写入，不能把它一起删掉
                if (samples.remove(keeper, sample)) {
                    logger.debug("[removeUnusedKeepers] remove {}, unused: {}", keeper, unused);
                }
            }
        }
    }

    /** 口径同 {@code DefaultKeeperSessionManager}：null 表示 meta 不可用、无法判定在用集合 */
    private Set<HostPort> getInUseKeepers() {
        XpipeMeta xpipeMeta = null == metaCache ? null : metaCache.getXpipeMeta();
        if (null == keeperSelector || null == xpipeMeta || null == xpipeMeta.getDcs()
                || xpipeMeta.getDcs().isEmpty()) {
            return null;
        }
        Set<HostPort> inUse = new HashSet<>();
        for (KeeperMeta keeperMeta : keeperSelector.select(xpipeMeta)) {
            inUse.add(new HostPort(keeperMeta.getIp(), keeperMeta.getPort()));
        }
        return inUse;
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

    @VisibleForTesting
    KeeperStateCache setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
        return this;
    }

    @VisibleForTesting
    KeeperStateCache setKeeperSelector(KeeperCheckSelector keeperSelector) {
        this.keeperSelector = keeperSelector;
        return this;
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
