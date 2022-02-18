package com.ctrip.xpipe.redis.console.sentinel.impl;

import com.ctrip.xpipe.command.AbstractCommand;
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

    public DefaultSentinelBindTask(SentinelManager sentinelManager, DcClusterShardService dcClusterShardService, List<SentinelGroupModel> sentinelGroupModels, ConsoleConfig config, MetaCache metaCache) {
        this.sentinelManager = sentinelManager;
        this.dcClusterShardService = dcClusterShardService;
        this.sentinelGroupModels = sentinelGroupModels;
        this.config = config;
        this.metaCache = metaCache;
    }

    @Override
    public String getName() {
        return "DefaultSentinelBindTask";
    }

    @Override
    protected void doExecute() throws Throwable {
        sentinelGroupModels.forEach(this::bindSentinelGroupWithShards);
        future().setSuccess();
    }

    void bindSentinelGroupWithShards(SentinelGroupModel sentinelGroupModel) {
        logger.debug("DefaultSentinelBindTask: sentinel group: {}",sentinelGroupModel.toString());
        List<SentinelInstanceModel> sentinelInstanceModels = sentinelGroupModel.getSentinels();
        List<Sentinel> sentinels = sentinelInstanceModels.stream().
                map(sentinelInstanceModel -> new Sentinel(String.format("%s:%d", sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort()), sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort())).
                collect(Collectors.toList());
        Set<String> monitorNames = new HashSet<>();
        sentinels.forEach(sentinel -> {
            monitorNames.addAll(getSentinelMonitorNames(sentinel));
        });
        logger.debug("DefaultSentinelBindTask: monitorNames: {}", monitorNames.toString());
        monitorNames.forEach(monitorName -> {
            bindSentinelGroupWithShard(sentinelGroupModel, monitorName);
        });
    }

    List<String> getSentinelMonitorNames(Sentinel sentinel) {
        String infoSentinel = sentinelManager.infoSentinel(sentinel);
        SentinelMonitors sentinelMonitors = SentinelMonitors.parseFromString(infoSentinel);
        return sentinelMonitors.getMonitors();
    }

    void bindSentinelGroupWithShard(SentinelGroupModel sentinelGroupModel, String monitorName) {
        try {
            SentinelUtil.SentinelInfo sentinelInfo = SentinelUtil.SentinelInfo.fromMonitorName(monitorName);
            String clusterName = sentinelInfo.getClusterName();
            String shardName = sentinelInfo.getShardName();
            String dcInMonitorName = sentinelInfo.getIdc();

            logger.debug("DefaultSentinelBindTask: bindSentinelGroupWithShard: {}", monitorName.toString());
            Map<String, ShardMeta> dcShards = dcShards(dcInMonitorName, shardName, clusterName);
            logger.debug("DefaultSentinelBindTask: dcShards: {}", dcShards);

            dcShards.forEach((dc, shardMeta) -> {
                logger.debug("DefaultSentinelBindTask: shardSentinelId: {}, sentinelGroupId:{}",shardMeta.getSentinelId(),sentinelGroupModel.getSentinelGroupId());
                if (shardMeta.getSentinelId() != sentinelGroupModel.getSentinelGroupId()) {
                    try {
                        DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find(clusterName, shardName, dc);
                        logger.debug("DefaultSentinelBindTask: dcClusterShardTbl of {}:{}:{}, id: {}",dc,clusterName,shardName,dcClusterShardTbl.getDcClusterShardId());
                        if (dcClusterShardTbl.getSetinelId() != sentinelGroupModel.getSentinelGroupId()) {
                            dcClusterShardService.updateDcClusterShard(dcClusterShardTbl.setSetinelId(sentinelGroupModel.getSentinelGroupId()));
                            logger.debug("DefaultSentinelBindTask: updateDcClusterShard of {}:{}:{}, id: {}",dc,clusterName,shardName,dcClusterShardTbl.getDcClusterShardId());
                        }
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
