package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.aggregator;

import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCollector;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public abstract class AbstractAggregationCollector<T extends SentinelHelloCollector> implements SentinelHelloCollector {

    private T realCollector;

    protected static Logger logger = LoggerFactory.getLogger(AbstractAggregationCollector.class);

    protected Set<HostPort> checkFinishedInstance = new HashSet<>();

    protected Set<HostPort> checkFailInstance = new HashSet<>();

    protected Set<SentinelHello> checkResult = new HashSet<>();

    protected final String clusterId;

    protected final String shardId;

    protected CheckerConfig checkerConfig;

    protected AtomicBoolean needDowngrade = new AtomicBoolean(false);

    public AbstractAggregationCollector(T sentinelHelloCollector, String clusterId, String shardId, CheckerConfig config) {
        this.realCollector = sentinelHelloCollector;
        this.clusterId = clusterId;
        this.shardId = shardId;
        this.checkerConfig = config;
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

                int collectedInstanceCount = collectHello(context);

                if (allInstancesCollected(collectedInstanceCount)) {
                    if (!needDowngrade.get() && shouldDowngrade(info)) {
                        logger.warn("[{}-{}+{}]backup dc or slaves {} sub failed, try to sub from active dc or master", LOG_TITLE, clusterId, shardId, info.getDcId());
                        beginDowngrade();
                    } else {
                        logger.debug("[{}-{}+{}]sub finish: {}", LOG_TITLE, clusterId, shardId, checkResult.toString());
                        handleAllHellos(context.instance());
                        endDowngrade();
                    }
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

    abstract protected boolean shouldCheckFromRedis(RedisHealthCheckInstance instance);

    abstract protected boolean allInstancesCollectedInDowngradeStatus(int collectedInstanceCount);

    abstract protected boolean allInstancesCollectedInNormalStatus(int collectedInstanceCount);

    private boolean allInstancesCollected(int collectedInstanceCount) {
        return needDowngrade.get() && allInstancesCollectedInDowngradeStatus(collectedInstanceCount) ||
                !needDowngrade.get() && allInstancesCollectedInNormalStatus(collectedInstanceCount);
    }

    private boolean shouldDowngrade(RedisInstanceInfo info) {
        if (checkFinishedInstance.isEmpty())
            return false;
        DowngradeStrategy strategy = DowngradeStrategy.lookUp(checkerConfig.sentinelCheckDowngradeStrategy());
        boolean shouldDowngrade = strategy.needDowngrade(checkResult, checkerConfig.getDefaultSentinelQuorumConfig());
        if (shouldDowngrade)
            CatEventMonitor.DEFAULT.logEvent("sentinel.check.downgrade", String.format("%s-%s-%s", strategy.name(), info.getClusterId(), info.getShardId()));
        return shouldDowngrade;
    }

    private void beginDowngrade() {
        needDowngrade.compareAndSet(false, true);
        resetCheckResult();
    }

    private void endDowngrade() {
        needDowngrade.compareAndSet(true, false);
    }


    @Override
    public void stopWatch(HealthCheckAction action) {
        resetCheckResult();
        this.needDowngrade.set(false);
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

    protected synchronized int collectHello(SentinelActionContext context) {
        RedisInstanceInfo info = context.instance().getCheckInfo();
        checkFinishedInstance.add(info.getHostPort());
        if (context.isSuccess()) checkResult.addAll(context.getResult());
        else checkFailInstance.add(info.getHostPort());

        return checkFinishedInstance.size();
    }

    protected synchronized void handleAllHellos(RedisHealthCheckInstance instance) {
        if (checkFinishedInstance.isEmpty())
            return;
        Set<SentinelHello> hellos = new HashSet<>(checkResult);
        resetCheckResult();
        this.realCollector.onAction(new SentinelActionContext(instance, hellos));
    }

    protected synchronized void resetCheckResult() {
        this.checkFinishedInstance.clear();
        this.checkFailInstance.clear();
        this.checkResult.clear();
    }

    @VisibleForTesting
    public boolean getNeedDowngrade() {
        return needDowngrade.get();
    }
}
