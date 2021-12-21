package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.AbstractAggregationCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.DefaultSentinelHelloCollector;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class SentinelCheckDowngradeCollectorController extends AbstractAggregationCollector<DefaultSentinelHelloCollector> implements OneWaySupport, SentinelActionController {

    protected static Logger logger = LoggerFactory.getLogger(SentinelCheckDowngradeCollectorController.class);

    private AtomicBoolean needDowngrade = new AtomicBoolean(false);

    private AtomicBoolean activeDcCollected = new AtomicBoolean(false);

    private MetaCache metaCache;

    private CheckerConfig checkerConfig;

    public SentinelCheckDowngradeCollectorController(MetaCache metaCache, DefaultSentinelHelloCollector sentinelHelloCollector, String clusterId, String shardId, CheckerConfig checkerConfig) {
        super(sentinelHelloCollector, clusterId, shardId);
        this.metaCache = metaCache;
        this.checkerConfig = checkerConfig;
    }

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        activeDcCollected.compareAndSet(true, false);
        return shouldCheckFromRedis(instance);
    }

    @Override
    public void onAction(SentinelActionContext context) {
        TransactionMonitor transaction = TransactionMonitor.DEFAULT;
        RedisInstanceInfo info = context.instance().getCheckInfo();
        transaction.logTransactionSwallowException("sentinel.check.notify", info.getClusterId(), new Task() {
            @Override
            public void go() throws Exception {
                if (!info.getClusterId().equalsIgnoreCase(clusterId) || !info.getShardId().equalsIgnoreCase(shardId))
                    return;
                if (!shouldCheckFromRedis(context.instance())) return;

                if (info.isInActiveDc() && activeDcCollected.compareAndSet(false, true)) {
                    // only deal with success result when downgrade
                    needDowngrade.compareAndSet(true, false);
                    if (context.isSuccess()) {
                        logger.info("[{}-{}+{}]active dc {} redis {} sub finish", LOG_TITLE, clusterId, shardId, info.getDcId(), info.getHostPort());
                        handleAllActiveDcHellos(context.instance(), context.getResult());
                    }
                    return;
                }

                // handle backup dc hello when all right
                if (info.isInActiveDc()) return;
                if (collectHello(context) >= countBackDcRedis()) {
                    if (shouldDowngrade(info)) {
                        logger.warn("[{}-{}+{}]backup dc {} sub failed, try to sub from active dc", LOG_TITLE, clusterId, shardId, info.getDcId());
                        beginDowngrade();
                        return;
                    }
                    needDowngrade.compareAndSet(true, false);
                    logger.debug("[{}-{}+{}]backup dc {} sub finish: {}", LOG_TITLE, clusterId, shardId, info.getDcId(), checkResult.toString());
                    handleAllBackupDcHellos(context.instance());
                }
            }

            @Override
            public Map<String, Object> getData() {
                Map<String, Object> transactionData = new HashMap<>();
                transactionData.put("checkRedisInstances", info);
                return transactionData;
            }
        });
    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        super.stopWatch(action);
        needDowngrade.set(false);
    }

    private boolean shouldDowngrade(RedisInstanceInfo info) {
        DowngradeStrategy strategy = DowngradeStrategy.lookUp(checkerConfig.sentinelCheckDowngradeStrategy());
        boolean shouldDowngrade = strategy.needDowngrade(checkResult, checkerConfig.getDefaultSentinelQuorumConfig());
        if (shouldDowngrade)
            CatEventMonitor.DEFAULT.logEvent("sentinel.check.downgrade", String.format("%s-%s-%s", strategy.name(), info.getClusterId(), info.getShardId()));
        return shouldDowngrade;
    }

    private boolean shouldCheckFromRedis(RedisHealthCheckInstance instance) {
        return noNeedDowngradeAndIsDrSlave(instance) || needDowngradeAndIsActiveDcSlave(instance);
    }

    private boolean noNeedDowngradeAndIsDrSlave(RedisHealthCheckInstance instance) {
        boolean shouldCheck = !needDowngrade.get() && !instance.getCheckInfo().isMaster() && !instance.getCheckInfo().isInActiveDc();

        logger.debug("[{}-{}+{}][{}]noNeedDowngradeAndIsDrSlave:{}, needDowngrade:{}, isMaster:{}, isInActiveDc:{}", LOG_TITLE, clusterId, shardId, instance.getCheckInfo().getHostPort(),
                shouldCheck, needDowngrade.get(), instance.getCheckInfo().isMaster(), instance.getCheckInfo().isInActiveDc());

        return shouldCheck;
    }

    private boolean needDowngradeAndIsActiveDcSlave(RedisHealthCheckInstance instance) {
        boolean shouldCheck = needDowngrade.get() && !instance.getCheckInfo().isMaster() && instance.getCheckInfo().isInActiveDc();

        logger.debug("[{}-{}+{}][{}]needDowngradeAndIsActiveDcSlave:{}, needDowngrade:{}, isMaster:{}", LOG_TITLE, clusterId, shardId, instance.getCheckInfo().getHostPort(),
                shouldCheck, needDowngrade.get(), instance.getCheckInfo().isMaster());

        return shouldCheck;
    }

    private int countBackDcRedis() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta || StringUtil.isEmpty(clusterId)) return 0;

        int redisCnt = 0;
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (!dcMeta.getClusters().containsKey(clusterId)) continue;
            if (dcMeta.getClusters().get(clusterId).getActiveDc().equalsIgnoreCase(dcMeta.getId())) continue;
            ShardMeta shardMeta = dcMeta.findCluster(clusterId).findShard(shardId);
            if (null == shardMeta) continue; // cluster missing shard when no instances in it
            redisCnt += shardMeta.getRedises().size();
        }

        return redisCnt;
    }

    private void beginDowngrade() {
        needDowngrade.compareAndSet(false, true);
        resetCheckResult();
    }

    enum DowngradeStrategy {
        lessThanHalf {
            @Override
            boolean needDowngrade(Set<SentinelHello> checkResult, QuorumConfig quorumConfig) {
                return extractSentinels(checkResult).size() < quorumConfig.getQuorum();
            }
        },
        lessThanAll {
            @Override
            boolean needDowngrade(Set<SentinelHello> checkResult, QuorumConfig quorumConfig) {
                return extractSentinels(checkResult).size() < quorumConfig.getTotal();
            }
        };

        Set<HostPort> extractSentinels(Set<SentinelHello> checkResult) {
            Set<HostPort> sentinels = new HashSet<>();
            for (SentinelHello sentinelHello : checkResult) {
                sentinels.add(sentinelHello.getSentinelAddr());
            }
            return sentinels;
        }

        abstract boolean needDowngrade(Set<SentinelHello> checkResult, QuorumConfig quorumConfig);

        public static DowngradeStrategy lookUp(String strategyName) {
            if (StringUtil.isEmpty(strategyName))
                throw new IllegalArgumentException("no DowngradeStrategy for name " + strategyName);
            return valueOf(strategyName);
        }
    }

}
