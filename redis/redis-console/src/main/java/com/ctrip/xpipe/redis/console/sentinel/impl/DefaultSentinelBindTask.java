package com.ctrip.xpipe.redis.console.sentinel.impl;

import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.healthcheck.allleader.sentinel.SentinelMonitors;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.console.model.SentinelInstanceModel;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBindTask;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;

import java.util.*;
import java.util.stream.Collectors;

public class DefaultSentinelBindTask extends AbstractCommand<Void> implements SentinelBindTask {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private SentinelManager sentinelManager;

    private DcClusterShardService dcClusterShardService;

    private List<SentinelGroupModel> sentinelGroupModels;

    private ConsoleConfig config;

    private MetaCache metaCache;

    private ClusterType clusterType;

    private static final String LOG_TYPE="sentinel.bind";

    public DefaultSentinelBindTask(SentinelManager sentinelManager, DcClusterShardService dcClusterShardService, List<SentinelGroupModel> sentinelGroupModels, ClusterType clusterType, ConsoleConfig config, MetaCache metaCache) {
        this.sentinelManager = sentinelManager;
        this.dcClusterShardService = dcClusterShardService;
        this.sentinelGroupModels = sentinelGroupModels;
        this.clusterType = clusterType;
        this.config = config;
        this.metaCache = metaCache;
    }

    @Override
    public String getName() {
        return "DefaultSentinelBindTask";
    }

    @Override
    protected void doExecute() throws Throwable {
        TransactionMonitor transaction = TransactionMonitor.DEFAULT;
        transaction.logTransaction(String.format("%s.%s", LOG_TYPE, "type"), clusterType.name(), new Task() {

            @Override
            public void go() throws Exception {
                sentinelGroupModels.forEach(sentinelGroupModel ->  bindSentinelGroupWithShards(sentinelGroupModel));
                future().setSuccess();
            }

            @Override
            public Map<String, Object> getData() {
                Map<String, Object> transactionData = new HashMap<>();
                transactionData.put("sentinel.groups", sentinelGroupModels);
                return transactionData;
            }
        });

    }

    void bindSentinelGroupWithShards(SentinelGroupModel sentinelGroupModel) {
        TransactionMonitor transaction = TransactionMonitor.DEFAULT;
        transaction.logTransactionSwallowException(String.format("%s.%s",LOG_TYPE,"group"), sentinelGroupModel.getSentinelsAddressString(), new Task() {

            Set<String> monitorNames = new HashSet<>();

            @Override
            public void go() throws Exception {
                try {
                    List<SentinelInstanceModel> sentinelInstanceModels = sentinelGroupModel.getSentinels();
                    List<Sentinel> sentinels = sentinelInstanceModels.stream().
                            map(sentinelInstanceModel -> new Sentinel(String.format("%s:%d", sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort()), sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort())).
                            collect(Collectors.toList());

                    sentinels.forEach(sentinel -> {
                        monitorNames.addAll(getSentinelMonitorNames(sentinel));
                    });
                    monitorNames.forEach(monitorName -> {
                        bindSentinelGroupWithShard(sentinelGroupModel, monitorName);
                    });
                } catch (Exception e) {
                    logger.error("[{}]bind sentinel for group {}", LOG_TYPE, sentinelGroupModel, e);
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
        String infoSentinel = sentinelManager.infoSentinel(sentinel);
        if (Strings.isNullOrEmpty(infoSentinel)) {
            logger.error("");
            return Lists.newArrayList();
        }
        SentinelMonitors sentinelMonitors = SentinelMonitors.parseFromString(infoSentinel);
        return sentinelMonitors.getMonitors();
    }

    void bindSentinelGroupWithShard(SentinelGroupModel sentinelGroupModel, String monitorName) {

        TransactionMonitor transaction = TransactionMonitor.DEFAULT;
        transaction.logTransactionSwallowException(String.format("%s.%s",LOG_TYPE,"monitorName"), monitorName, new Task() {
            SentinelUtil.SentinelInfo sentinelInfo = SentinelUtil.SentinelInfo.fromMonitorName(monitorName);
            String clusterName = sentinelInfo.getClusterName();
            String shardName = sentinelInfo.getShardName();
            String dcInMonitorName = sentinelInfo.getIdc();

            Map<String, ShardMeta> dcShards = dcShards(dcInMonitorName, clusterName, shardName);
            @Override
            public void go() throws Exception {
                try {
                    dcShards.forEach((dc, shardMeta) -> {
                        if (shardMeta.getSentinelId() != sentinelGroupModel.getSentinelGroupId()) {
                            try {
                                DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find(dc,clusterName, shardName);
                                if (dcClusterShardTbl.getSetinelId() != sentinelGroupModel.getSentinelGroupId()) {
                                    dcClusterShardService.updateDcClusterShard(dcClusterShardTbl.setSetinelId(sentinelGroupModel.getSentinelGroupId()));
                                }
                                CatEventMonitor.DEFAULT.logEvent(LOG_TYPE, String.format("%s: sentinelId:%d->%d", monitorName, shardMeta.getSentinelId(), sentinelGroupModel.getSentinelGroupId()));
                                logger.info("bind sentinel group {} with {} {} {} ,dcClusterShardId:{}", sentinelGroupModel.getSentinelGroupId(), dc, clusterName, shardName, dcClusterShardTbl.getDcClusterShardId());
                            } catch (DalException e) {
                                logger.error("bind sentinel group {} with {} {} {} failed", sentinelGroupModel.getSentinelGroupId(), dc, clusterName, shardName, e);
                            }
                        }
                    });
                } catch (Exception e) {
                    logger.error("handle monitor name {} failed", monitorName, e);
                }
            }

            @Override
            public Map<String, Object> getData() {
                Map<String, Object> transactionData = new HashMap<>();
                transactionData.put("dcShards", dcShards);
                return transactionData;
            }
        });

    }

    Map<String, ShardMeta> dcShards(String dcInMonitorName, String clusterName, String shardName) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        Map<String, ShardMeta> dcShards = new HashMap<>();
        if (dcInMonitorName.equalsIgnoreCase(config.crossDcSentinelMonitorNameSuffix())) {
            xpipeMeta.getDcs().forEach((dc, dcMeta) -> {
                ClusterMeta clusterMeta = dcMeta.findCluster(clusterName);
                if (clusterMeta != null) {
                    dcShards.put(dc, clusterMeta.findShard(shardName));
                }
            });
        } else {
            dcShards.put(dcInMonitorName, xpipeMeta.getDcs().get(dcInMonitorName).findCluster(clusterName).findShard(shardName));
        }
        return dcShards;
    }

    @Override
    protected void doReset() {

    }

}
