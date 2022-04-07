package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Set;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class AnalyseHellos extends AbstractSentinelHelloCollectCommand {

    private CheckerConfig checkerConfig;

    public AnalyseHellos(SentinelHelloCollectContext context, CheckerConfig checkerConfig) {
        super(context);
        this.checkerConfig = checkerConfig;
    }

    @Override
    protected void doExecute() throws Throwable {
        // check wrong master hellos
        context.getToDelete().addAll(checkWrongMasterHellos(context.getProcessedHellos(), context.getTrueMasterInfo().getKey()));

        // to check reset
        Set<SentinelHello> toCheckReset = Sets.newHashSet(context.getProcessedHellos());
        context.setToCheckReset(ignoreNetWorkErrorHellos(toCheckReset));

        // check add,ignore network error sentinels
        Set<SentinelHello> toAdd = checkToAdd(context.getInfo().getClusterId(), context.getInfo().getShardId(),
                context.getSentinelMonitorName(), context.getSentinels(), context.getProcessedHellos(), context.getTrueMasterInfo().getKey(),
                checkerConfig.getDefaultSentinelQuorumConfig());
        context.setToAdd(ignoreNetWorkErrorHellos(toAdd));

        future().setSuccess();
    }

    @VisibleForTesting
    Set<SentinelHello> checkWrongMasterHellos(Set<SentinelHello> hellos, HostPort trueMaster) {
        Set<SentinelHello> wrongMasters = new HashSet<>();
        for (SentinelHello sentinelHello : hellos) {
            if (!sentinelHello.getMasterAddr().equals(trueMaster)) {
                wrongMasters.add(sentinelHello);
            }
        }
        hellos.removeAll(wrongMasters);
        return wrongMasters;
    }

    @VisibleForTesting
    Set<SentinelHello> checkToAdd(String clusterId, String shardId, String sentinelMonitorName, Set<HostPort> sentinels, Set<SentinelHello> hellos, HostPort masterAddr, QuorumConfig quorumConfig) {

        if (masterAddr == null) {
            logger.warn("[{}-{}][checkToAdd][no master]", LOG_TITLE, sentinelMonitorName);
            return Sets.newHashSet();
        }

        if (StringUtil.isEmpty(sentinelMonitorName)) {
            logger.warn("[{}-{}+{}][checkToAdd][no monitor name]", LOG_TITLE, clusterId, shardId);
            return Sets.newHashSet();
        }

        if (hellos.size() >= quorumConfig.getTotal()) {
            return Sets.newHashSet();
        }

        Set<HostPort> currentSentinels = new HashSet<>();
        hellos.forEach((hello -> currentSentinels.add(hello.getSentinelAddr())));

        Set<SentinelHello> toAdd = new HashSet<>();
        int toAddSize = quorumConfig.getTotal() - hellos.size();

        int i = 0;
        for (HostPort hostPort : sentinels) {
            if (!currentSentinels.contains(hostPort)) {
                i++;
                if (i > toAddSize) {
                    break;
                }
                toAdd.add(new SentinelHello(hostPort, masterAddr, sentinelMonitorName));
            }
        }
        return toAdd;
    }

    Set<SentinelHello> ignoreNetWorkErrorHellos(Set<SentinelHello> sentinelHellos) {
        Set<SentinelHello> ignoreReset = new HashSet<>();
        sentinelHellos.forEach(toCheckSentinel -> {
            if (context.getNetworkErrorSentinels().containsKey(toCheckSentinel.getSentinelAddr()))
                ignoreReset.add(toCheckSentinel);
        });
        sentinelHellos.removeAll(ignoreReset);
        return sentinelHellos;
    }
}