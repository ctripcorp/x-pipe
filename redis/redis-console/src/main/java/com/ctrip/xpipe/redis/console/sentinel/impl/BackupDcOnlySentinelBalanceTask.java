package com.ctrip.xpipe.redis.console.sentinel.impl;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
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

    protected List<SetinelTbl> busySentinels;

    public BackupDcOnlySentinelBalanceTask(String dcId, SentinelBalanceService sentinelBalanceService, DcClusterShardService dcClusterShardService) {
        this.dcId = dcId;
        this.sentinelBalanceService = sentinelBalanceService;
        this.dcClusterShardService = dcClusterShardService;
        this.initTask();
    }

    protected void initTask() {
        this.busySentinels = new LinkedList<>();

        long totalUsages = 0;
        List<SetinelTbl> sentinels = sentinelBalanceService.getCachedDcSentinel(dcId);
        for (SetinelTbl sentinel: sentinels) {
            totalUsages += sentinel.getShardCount();
        }

        if (sentinels.isEmpty()) {
            this.targetUsage = 0;
        } else {
            this.targetUsage = (int)Math.ceil(totalUsages*1.0D/sentinels.size());
        }

        for (SetinelTbl sentinel: sentinels) {
            if (sentinel.getShardCount() > targetUsage) {
                busySentinels.add(sentinel);
            }
        }
    }

    @Override
    public int getShardsWaitBalances() {
        if (null == busySentinels || busySentinels.isEmpty()) return 0;

        int total = 0;
        for (SetinelTbl sentinel: busySentinels) {
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
        for (SetinelTbl setinelTbl: busySentinels) {
            if (future().isCancelled()) {
                getLogger().info("[rebalanceBackupDcSentinels]{} unfinish but cancle", getName());
                return;
            }
            rebalanceBackupDcSentinel(setinelTbl);
        }
    }

    protected void doBalanceSentinel(SetinelTbl setinelTbl, List<DcClusterShardTbl> dcClusterShards) {
        dcClusterShards.forEach(dcClusterShard -> {
            SetinelTbl suitableSentinel = sentinelBalanceService.selectSentinelWithoutCache(dcId);
            if (null == suitableSentinel) {
                getLogger().info("[doBalanceSentinel]{} none sentinel selected", getName());
                throw new NoSentinelsToUseException(getName() + "none sentinel selected");
            } else if (suitableSentinel.getSetinelId() == setinelTbl.getSetinelId()) {
                getLogger().info("[doBalanceSentinel] no other sentinel to use");
                throw new NoSentinelsToUseException(getName() + "no other sentinel to use");
            }

            dcClusterShard.setSetinelId(suitableSentinel.getSetinelId());
            try {
                dcClusterShardService.updateDcClusterShard(dcClusterShard);
                setinelTbl.setShardCount(setinelTbl.getShardCount() - 1);
            } catch (DalException e) {
                getLogger().info("[doBalanceSentinel][fail, skip] {}", dcClusterShard, e);
            }
        });
    }

    private void rebalanceBackupDcSentinel(SetinelTbl sentinelTbl) {
        int needBalanceShards = (int)(sentinelTbl.getShardCount() - targetUsage);
        if (needBalanceShards <= 0) {
            getLogger().info("[rebalanceSentinel][no need, skip] {}", sentinelTbl.getSetinelId());
            return;
        }

        List<DcClusterShardTbl> dcClusterShardTbls = dcClusterShardService.findBackupDcShardsBySentinel(sentinelTbl.getSetinelId());
        doBalanceSentinel(sentinelTbl, dcClusterShardTbls.subList(0, Math.min(needBalanceShards, dcClusterShardTbls.size())));
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
