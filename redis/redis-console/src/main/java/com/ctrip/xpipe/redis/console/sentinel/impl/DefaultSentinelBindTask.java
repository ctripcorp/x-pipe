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
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DefaultSentinelBindTask extends AbstractCommand<Void> implements SentinelBindTask {

    private Logger logger= LoggerFactory.getLogger(getClass());

    private SentinelManager sentinelManager;

    private DcClusterShardService dcClusterShardService;

    private List<SentinelGroupModel> sentinelGroupModels;

    private ConsoleConfig config;

    public DefaultSentinelBindTask(SentinelManager sentinelManager, DcClusterShardService dcClusterShardService, List<SentinelGroupModel> sentinelGroupModels, ConsoleConfig config) {
        this.sentinelManager = sentinelManager;
        this.dcClusterShardService = dcClusterShardService;
        this.sentinelGroupModels = sentinelGroupModels;
        this.config = config;
    }

    @Override
    public String getName() {
        return "DefaultSentinelBindTask";
    }

    @Override
    protected void doExecute() throws Throwable {
        sentinelGroupModels.forEach(this::bindSentinelGroupWithShards);
    }

    void bindSentinelGroupWithShards(SentinelGroupModel sentinelGroupModel) {
        List<SentinelInstanceModel> sentinelInstanceModels = sentinelGroupModel.getSentinels();
        List<Sentinel> sentinels = sentinelInstanceModels.stream().
                map(sentinelInstanceModel -> new Sentinel(String.format("%s:%d", sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort()), sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort())).
                collect(Collectors.toList());
        List<String> monitorNames = new ArrayList<>();
        sentinels.forEach(sentinel -> {
            String infoSentinel = sentinelManager.infoSentinel(sentinel);
            SentinelMonitors sentinelMonitors = SentinelMonitors.parseFromString(infoSentinel);
            monitorNames.addAll(sentinelMonitors.getMonitors());
        });
        monitorNames.forEach(monitorName -> {
            bindSentinelGroupWithShard(sentinelGroupModel, monitorName);
        });
    }

    void bindSentinelGroupWithShard(SentinelGroupModel sentinelGroupModel, String monitorName) {
        try {
            SentinelUtil.SentinelInfo sentinelInfo = SentinelUtil.SentinelInfo.fromMonitorName(monitorName);
            String clusterName = sentinelInfo.getClusterName();
            String shardName = sentinelInfo.getShardName();
            String dcName = sentinelInfo.getIdc();
            List<DcClusterShardTbl> clusterShards = new ArrayList<>();

            if (dcName.equalsIgnoreCase(config.crossDcSentinelMonitorNameSuffix())) {
                clusterShards.addAll(dcClusterShardService.find(clusterName, shardName));
            } else {
                clusterShards.add(dcClusterShardService.find(clusterName, shardName, dcName));
            }

            clusterShards.forEach(dcClusterShardTbl -> {
                if (dcClusterShardTbl.getSetinelId() != sentinelGroupModel.getSentinelGroupId()) {
                    try {
                        dcClusterShardService.updateDcClusterShard(dcClusterShardTbl.setSetinelId(sentinelGroupModel.getSentinelGroupId()));
                    } catch (DalException e) {
                        logger.error("bind sentinel group {} with {} {} {} failed", sentinelGroupModel.getSentinelGroupId(), dcName, clusterName, shardName, e);
                    }
                }
            });
        } catch (Exception e) {
            logger.error("handle monitor name {} failed", monitorName, e);
        }
    }

    @Override
    protected void doReset() {

    }
}
