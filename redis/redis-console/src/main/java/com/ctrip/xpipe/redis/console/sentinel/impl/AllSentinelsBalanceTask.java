package com.ctrip.xpipe.redis.console.sentinel.impl;

import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceTask;
import com.ctrip.xpipe.redis.console.sentinel.exception.NoSentinelsToUseException;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author lishanglin
 * date 2021/8/31
 */
public class AllSentinelsBalanceTask extends BackupDcOnlySentinelBalanceTask implements SentinelBalanceTask {

    private ScheduledExecutorService scheduled;

    private ScheduledFuture<?> scheduledFuture;

    private int balanceBatch;

    private int balanceIntervalMilli;

    public AllSentinelsBalanceTask(String dcId, SentinelBalanceService sentinelBalanceService,
                                   DcClusterShardService dcClusterShardService, ScheduledExecutorService scheduled,
                                   int balanceBatch, int balanceIntervalMilli) {
        super(dcId, sentinelBalanceService, dcClusterShardService);
        this.scheduled = scheduled;
        this.balanceBatch = balanceBatch;
        this.balanceIntervalMilli = balanceIntervalMilli;
    }

    @Override
    protected void doExecute() throws Throwable {
        rebalanceBackupDcSentinels();

        if (null == findNextBusySentinels()) {
            getLogger().info("[doExecute]{} nothing to do after rebalanceBackupDcSentinels", getName());
            future().setSuccess();
        } else {
            getLogger().info("[doExecute]{} continue balance active dc sentinels", getName());
            this.scheduledFuture = scheduled.scheduleWithFixedDelay(this::balanceSentinel, 0, balanceIntervalMilli, TimeUnit.MILLISECONDS);
        }
    }

    private SentinelGroupModel findNextBusySentinels() {
        for (SentinelGroupModel sentinelGroupModel: busySentinels) {
            if (sentinelGroupModel.getShardCount() > targetUsage) return sentinelGroupModel;
        }

        return null;
    }

    private void balanceSentinel() {
        if (future().isCancelled() && null != scheduledFuture && !scheduledFuture.isDone()) {
            scheduledFuture.cancel(false);
            return;
        }

        try {
            balanceNextSentinel();
        } catch (NoSentinelsToUseException e) {
            future().setFailure(e);
            this.scheduledFuture.cancel(false);
        } catch (Throwable th) {
            getLogger().warn("[balanceSentinel]{} fail", getName(), th);
        }

        if (null == findNextBusySentinels()) setSuccess();
    }

    private void balanceNextSentinel() {
        SentinelGroupModel sentinelGroup = findNextBusySentinels();
        if (null == sentinelGroup) return;

        List<DcClusterShardTbl> relatedShards = dcClusterShardService.findAllShardsBySentinel(sentinelGroup.getSentinelGroupId());

        if (relatedShards.size() > targetUsage) {
            int batch = Math.min(balanceBatch, relatedShards.size() - targetUsage);
            getLogger().debug("[balanceSentinel]{} sentinel:{} batch:{}", getName(), sentinelGroup.getSentinelGroupId(), batch);
            doBalanceSentinel(sentinelGroup, relatedShards.subList(0, batch));
        } else {
            sentinelGroup.setShardCount(relatedShards.size());
        }
    }

    private void setSuccess() {
        if (!future().isDone()) {
            future().setSuccess();
        }
        if (null != scheduledFuture && !scheduledFuture.isDone()) {
            scheduledFuture.cancel(false);
        }
    }

    @Override
    protected void doCancel() {
        super.doCancel();
        if (null != scheduledFuture && !this.scheduledFuture.isDone()) {
            this.scheduledFuture.cancel(true);
        }
    }

}
