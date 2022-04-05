package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.core.exception.SentinelsException;
import com.ctrip.xpipe.redis.core.protocal.pojo.SentinelFlag;
import com.google.common.util.concurrent.MoreExecutors;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class CheckFailoverInProgress extends AbstractSentinelHelloCollectCommand {

    private SentinelManager sentinelManager;

    public CheckFailoverInProgress(SentinelHelloCollectContext context, SentinelManager sentinelManager) {
        super(context);
        this.sentinelManager = sentinelManager;
    }

    @Override
    protected void doExecute() throws Throwable {
        collectAllMasters();

        if (currentMasterConsistent(context.getAllMasters())) {
            logger.debug("[{}-{}+{}] {} unique master in CheckFailoverInProgress step : {}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getSentinelMonitorName(), context.getAllMasters().iterator().next());
            future().setSuccess();
        } else {
            logger.warn("[{}-{}+{}]collected masters not unique in CheckFailoverInProgress step: {}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getAllMasters());

            ParallelCommandChain sentinelMasterCommandChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
            context.getHellos().forEach(sentinelHello -> {
                if (notCollected(sentinelHello.getSentinelAddr())) {
                    sentinelMasterCommandChain.add(sentinelMaster(sentinelManager, sentinelHello.getSentinelAddr()));
                }
            });

            sentinelMasterCommandChain.execute().addListener(future -> {
                if (failoverInProgress()) {
                    future().setFailure(new SentinelsException(String.format("%s failover in progress", context.getSentinelMonitorName())));
                } else {
                    replaceHelloMastersWithSentinelMasters();
                    future().setSuccess();
                }
            });
        }
    }

    boolean failoverInProgress() {
        for (HostPort sentinel : context.getSentinelMonitors().keySet()) {
            if (context.getSentinelMonitors().get(sentinel).flags().contains(SentinelFlag.failover_in_progress)) {
                String msg = String.format("fail-in-progress: %s, %s, stop check", sentinel, context.getSentinelMonitorName());
                logger.warn("[{}-{}+{}]{}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), msg);
                return true;
            }
        }
        return false;
    }

    void replaceHelloMastersWithSentinelMasters() {
        context.getSentinelMonitors().forEach((sentinel, sentinelMaster) -> {
            context.getHellos().forEach(sentinelHello -> {
                if (sentinelHello.getSentinelAddr().equals(sentinel))
                    sentinelHello.setMasterAddr(sentinelMaster.getHostPort());
            });
        });
    }

    boolean notCollected(HostPort sentinelAddr) {
        return !context.getSentinelMonitors().containsKey(sentinelAddr);
    }

}