package com.ctrip.xpipe.redis.console.sentinel.impl;

import com.ctrip.xpipe.cluster.SentinelType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceTask;
import com.ctrip.xpipe.redis.console.sentinel.exception.NoSentinelsToUseException;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import org.unidal.dal.jdbc.DalException;

import java.util.LinkedList;
import java.util.List;

/**
 * @author lishanglin
 * date 2021/9/1
 */
public class BackupDcOnlySentinelBalanceTask extends AbstractCommand<Void> implements SentinelBalanceTask {

    protected String dcId;

    protected SentinelBalanceService sentinelBalanceService;

    protected DcClusterShardService dcClusterShardService;

    protected int targetUsage;

    protected List<SentinelGroupModel> busySentinels;

    public BackupDcOnlySentinelBalanceTask(String dcId, SentinelBalanceService sentinelBalanceService, DcClusterShardService dcClusterShardService) {
        this.dcId = dcId;
        this.sentinelBalanceService = sentinelBalanceService;
        this.dcClusterShardService = dcClusterShardService;
        this.initTask();
    }

    protected void initTask() {
        this.busySentinels = new LinkedList<>();

        long totalUsages = 0;
        List<SentinelGroupModel> sentinels = sentinelBalanceService.getCachedDcSentinel(dcId, SentinelType.DR_CLUSTER);
        for (SentinelGroupModel sentinel: sentinels) {
            totalUsages += sentinel.getShardCount();
        }

        if (sentinels.isEmpty()) {
            this.targetUsage = 0;
        } else {
            this.targetUsage = (int)Math.ceil(totalUsages*1.0D/sentinels.size());
        }

        for (SentinelGroupModel sentinel: sentinels) {
            if (sentinel.getShardCount() > targetUsage) {
                busySentinels.add(sentinel);
            }
        }
    }

    @Override
    public int getShardsWaitBalances() {
        if (null == busySentinels || busySentinels.isEmpty()) return 0;

        int total = 0;
        for (SentinelGroupModel sentinel: busySentinels) {
            int waitBalanceShards = (int)(sentinel.getShardCount() - targetUsage);
            total += Math.max(0, waitBalanceShards);
        }

        return total;
    }

    @Override
    public int getTargetUsages() {
        return targetUsage;
    }

    @Override
    protected void doExecute() throws Throwable {
        rebalanceBackupDcSentinels();
        future().setSuccess();
    }

    protected void rebalanceBackupDcSentinels() {
        for (SentinelGroupModel sentinelGroup: busySentinels) {
            if (future().isCancelled()) {
                getLogger().info("[rebalanceBackupDcSentinels]{} unfinish but cancle", getName());
                return;
            }
            rebalanceBackupDcSentinel(sentinelGroup);
        }
    }

    protected void doBalanceSentinel(SentinelGroupModel sentinelGroupModel, List<DcClusterShardTbl> dcClusterShards) {
        dcClusterShards.forEach(dcClusterShard -> {
            SentinelGroupModel suitableSentinel = sentinelBalanceService.selectSentinelWithoutCache(dcId, SentinelType.DR_CLUSTER);
            if (null == suitableSentinel) {
                getLogger().info("[doBalanceSentinel]{} none sentinel selected", getName());
                throw new NoSentinelsToUseException(getName() + "none sentinel selected");
            } else if (suitableSentinel.getSentinelGroupId() == sentinelGroupModel.getSentinelGroupId()) {
                getLogger().info("[doBalanceSentinel] no other sentinel to use");
                throw new NoSentinelsToUseException(getName() + "no other sentinel to use");
            }

            dcClusterShard.setSetinelId(suitableSentinel.getSentinelGroupId());
            try {
                dcClusterShardService.updateDcClusterShard(dcClusterShard);
                sentinelGroupModel.setShardCount(sentinelGroupModel.getShardCount() - 1);
            } catch (DalException e) {
                getLogger().info("[doBalanceSentinel][fail, skip] {}", dcClusterShard, e);
            }
        });
    }

    private void rebalanceBackupDcSentinel(SentinelGroupModel sentinelGroup) {
        int needBalanceShards = (int)(sentinelGroup.getShardCount() - targetUsage);
        if (needBalanceShards <= 0) {
            getLogger().info("[rebalanceSentinel][no need, skip] {}", sentinelGroup.getSentinelGroupId());
            return;
        }

        List<DcClusterShardTbl> dcClusterShardTbls = dcClusterShardService.findBackupDcShardsBySentinel(sentinelGroup.getSentinelGroupId());
        doBalanceSentinel(sentinelGroup, dcClusterShardTbls.subList(0, Math.min(needBalanceShards, dcClusterShardTbls.size())));
    }

    @Override
    protected void doReset() {
        initTask();
    }

    @Override
    public String getName() {
        return String.format("[%s-%s]", getClass().getSimpleName(), dcId);
    }

}
