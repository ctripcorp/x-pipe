package com.ctrip.xpipe.redis.checker.healthcheck.allleader.sentinel;

import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.utils.IpUtils;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DefaultSentinelBindTask extends AbstractCommand<Void> implements SentinelBindTask {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private SentinelManager sentinelManager;

    private DcMeta dcMeta;

    private CheckerConfig config;

    private ClusterType clusterType;

    private CheckerConsoleService checkerConsoleService;

    private static final String LOG_TYPE = "sentinel.bind";

    public DefaultSentinelBindTask(SentinelManager sentinelManager, DcMeta dcMeta, ClusterType clusterType, CheckerConsoleService checkerConsoleService, CheckerConfig config) {
        this.sentinelManager = sentinelManager;
        this.clusterType = clusterType;
        this.config = config;
        this.dcMeta = dcMeta;
        this.checkerConsoleService = checkerConsoleService;
    }

    @Override
    public String getName() {
        return "DefaultSentinelBindTask";
    }

    @Override
    protected void doExecute() throws Throwable {
        TransactionMonitor transaction = TransactionMonitor.DEFAULT;
        transaction.logTransaction(String.format("%s.%s", LOG_TYPE, "type"), clusterType.name(), new Task() {

            List<SentinelMeta> typeSentinels = new ArrayList<>();

            @Override
            public void go() throws Exception {
                Map<Long, SentinelMeta> dcSentinels = dcMeta.getSentinels();

                dcSentinels.values().forEach(sentinelMeta -> {
                    if (ClusterType.lookup(sentinelMeta.getClusterType()).equals(clusterType)) {
                        typeSentinels.add(sentinelMeta);
                    }
                });
                typeSentinels.forEach(sentinelMeta -> bindSentinelGroupWithShards(sentinelMeta));
                future().setSuccess();
            }

            @Override
            public Map<String, Object> getData() {
                Map<String, Object> transactionData = new HashMap<>();
                transactionData.put("sentinel.groups", typeSentinels);
                return transactionData;
            }
        });
    }

    void bindSentinelGroupWithShards(SentinelMeta sentinelMeta) {
        TransactionMonitor transaction = TransactionMonitor.DEFAULT;
        transaction.logTransactionSwallowException(String.format("%s.%s", LOG_TYPE, "group"), sentinelMeta.getAddress(), new Task() {

            Set<String> monitorNames = new HashSet<>();

            @Override
            public void go() throws Exception {
                try {
                    List<Sentinel> sentinels = IpUtils.parseAsHostPorts(sentinelMeta.getAddress()).stream().
                            map(hostPort -> new Sentinel(String.format("%s:%d", hostPort.getHost(), hostPort.getPort()), hostPort.getHost(), hostPort.getPort())).
                            collect(Collectors.toList());
                    sentinels.forEach(sentinel -> {
                        monitorNames.addAll(getSentinelMonitorNames(sentinel));
                    });
                    monitorNames.forEach(monitorName -> {
                        bindSentinelGroupWithShard(sentinelMeta, monitorName);
                    });
                } catch (Exception e) {
                    logger.error("[{}]bind sentinel for group {}", LOG_TYPE, sentinelMeta, e);
                }
            }

            @Override
            public Map<String, Object> getData() {
                Map<String, Object> transactionData = new HashMap<>();
                transactionData.put("monitorNames", monitorNames);
                return transactionData;
            }
        });
    }

    List<String> getSentinelMonitorNames(Sentinel sentinel) {
        String infoSentinel = null;
        try {
            infoSentinel = sentinelManager.infoSentinel(sentinel).execute().get(2050, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("[checkSentinel] infoSentinel failed: {}", sentinel, e);
        }
        if (Strings.isNullOrEmpty(infoSentinel)) {
            logger.error("info sentinel failed: {}", sentinel);
            return Lists.newArrayList();
        }
        SentinelMonitors sentinelMonitors = SentinelMonitors.parseFromString(infoSentinel);
        return sentinelMonitors.getMonitors();
    }

    void bindSentinelGroupWithShard(SentinelMeta sentinelMeta, String monitorName) {

        TransactionMonitor transaction = TransactionMonitor.DEFAULT;
        transaction.logTransactionSwallowException(String.format("%s.%s", LOG_TYPE, "monitorName"), monitorName, new Task() {
            SentinelUtil.SentinelInfo sentinelInfo = SentinelUtil.SentinelInfo.fromMonitorName(monitorName);
            String clusterName = sentinelInfo.getClusterName();
            String shardName = sentinelInfo.getShardName();

            ShardMeta dcShard ;

            @Override
            public void go() throws Exception {
                try {
                    dcShard = dcShard(clusterName, shardName);
                    if (dcShard != null && !dcShard.getSentinelId().equals(sentinelMeta.getId())) {
                        try {
                            checkerConsoleService.bindShardSentinel(config.getConsoleAddress(), dcMeta.getId(), clusterName, shardName, sentinelMeta);
                            CatEventMonitor.DEFAULT.logEvent(LOG_TYPE, String.format("%s: sentinelId:%d->%d", monitorName, dcShard.getSentinelId(), sentinelMeta.getId()));
                            logger.info("bind sentinel group {} with {} {} {} ,dcClusterShardId:{}", sentinelMeta.getId(), dcMeta.getId(), clusterName, shardName, sentinelMeta.getId());
                        } catch (Throwable e) {
                            logger.error("bind sentinel group {} with {} {} {} failed", sentinelMeta.getId(), dcMeta.getId(), clusterName, shardName, e);
                        }
                    }
                } catch (Exception e) {
                    logger.error("handle monitor name {} failed", monitorName, e);
                }
            }

            @Override
            public Map<String, Object> getData() {
                Map<String, Object> transactionData = new HashMap<>();
                transactionData.put("dcShard", dcShard);
                return transactionData;
            }
        });

    }

    ShardMeta dcShard(String clusterName, String shardName) {
        ClusterMeta clusterMeta = dcMeta.findCluster(clusterName);

        if (clusterMeta == null)
            return null;

        return clusterMeta.findShard(shardName);
    }

    @Override
    protected void doReset() {

    }

}
