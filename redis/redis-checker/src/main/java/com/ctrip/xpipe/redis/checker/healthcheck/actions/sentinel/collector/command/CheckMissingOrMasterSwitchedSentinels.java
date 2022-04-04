package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.core.exception.SentinelsException;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class CheckMissingOrMasterSwitchedSentinels extends AbstractSentinelHelloCollectCommand {

   private AlertManager alertManager;

   private CheckerConfig checkerConfig;

   private SentinelManager sentinelManager;

    public CheckMissingOrMasterSwitchedSentinels(SentinelHelloCollectContext context, AlertManager alertManager,
                                                 CheckerConfig checkerConfig, SentinelManager sentinelManager) {
        super(context);
        this.alertManager = alertManager;
        this.checkerConfig = checkerConfig;
        this.sentinelManager = sentinelManager;
    }

    @Override
    protected void doExecute() throws Throwable {
        if (sentinelMasterSwitched()) {
            future().setFailure(new SentinelsException(String.format("%s MasterSwitched", context.getSentinelMonitorName())));
        } else {
            Set<HostPort> missingSentinels = missingSentinels();
            if (missingSentinels.isEmpty())
                future().setSuccess();
            else {
                ParallelCommandChain chain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
                missingSentinels.forEach(sentinel -> chain.add(sentinelMaster(sentinelManager, sentinel)));
                chain.execute().addListener(future -> {
                    if (majoritySentinelsNetworkError()) {
                        logger.warn("[{}-{}+{}] {} lost connection to majority sentinels : {}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getSentinelMonitorName(), context.getNetworkErrorSentinels().keySet());
                        String message = String.format("monitorName:%s, network error sentinels :%s", context.getSentinelMonitorName(), context.getNetworkErrorSentinels().keySet());
                        alertManager.alert(context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getInfo().getHostPort(), ALERT_TYPE.MAJORITY_SENTINELS_NETWORK_ERROR, message);
                        future().setFailure(new SentinelsException(message));
                    } else {
                        addMissingSentinelMonitorsToHellos();
                        future().setSuccess();
                    }
                });
            }
        }
    }

    void addMissingSentinelMonitorsToHellos() {
        if (!context.getSentinelMonitors().isEmpty()) {
            context.getSentinelMonitors().forEach((sentinel, sentinelMater) -> {
                context.getHellos().add(new SentinelHello(sentinel, sentinelMater.getHostPort(), context.getSentinelMonitorName()));
            });
        }
    }

    boolean sentinelMasterSwitched() {
        Map<HostPort, Set<HostPort>> sentinelMasters = new HashMap<>();
        context.getHellos().forEach(sentinelHello -> {
            sentinelMasters.putIfAbsent(sentinelHello.getSentinelAddr(), Sets.newHashSet());
            sentinelMasters.get(sentinelHello.getSentinelAddr()).add(sentinelHello.getMasterAddr());
        });
        for (HostPort sentinel : sentinelMasters.keySet()) {
            if (sentinelMasters.get(sentinel).size() > 1) {
                String msg = String.format("sentinel %s switched master: %s in %s, stop check", sentinel, sentinelMasters.get(sentinel), context.getSentinelMonitorName());
                logger.warn("[{}-{}+{}]{}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), msg);
                return true;
            }
        }
        return false;
    }

    boolean majoritySentinelsNetworkError() {
        return context.getNetworkErrorSentinels().size() >= checkerConfig.getDefaultSentinelQuorumConfig().getQuorum();
    }

    Set<HostPort> missingSentinels() {
        Set<HostPort> missingSentinels = new HashSet<>();
        context.getSentinels().forEach(sentinel -> {
            if (!foundInHellos(sentinel))
                missingSentinels.add(sentinel);
        });
        return missingSentinels;
    }

    boolean foundInHellos(HostPort sentinel) {
        for (SentinelHello sentinelHello : context.getHellos()) {
            if (sentinelHello.getSentinelAddr().equals(sentinel))
                return true;
        }
        return false;
    }

}
