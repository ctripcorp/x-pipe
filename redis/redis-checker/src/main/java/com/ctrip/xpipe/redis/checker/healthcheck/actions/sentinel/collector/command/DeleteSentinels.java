package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.core.exception.SentinelsException;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class DeleteSentinels extends AbstractSentinelHelloCollectCommand {

    private SentinelManager sentinelManager;
    private boolean needCheckMasterSlaves;

    public DeleteSentinels(SentinelHelloCollectContext context, SentinelManager sentinelManager) {
        this(context, sentinelManager, true);
    }

    public DeleteSentinels(SentinelHelloCollectContext context, SentinelManager sentinelManager, boolean needCheckMasterSlaves) {
        super(context);
        this.sentinelManager = sentinelManager;
        this.needCheckMasterSlaves = needCheckMasterSlaves;
    }

    @Override
    protected void doExecute() throws Throwable {

        if (context.getToDelete().size() == 0) {
            future().setSuccess();
        } else {
            logger.info("[{}-{}][toDelete]{}", LOG_TITLE, context.getSentinelMonitorName(), context.getToDelete());

            if (shouldDelete()) {
                ParallelCommandChain deleteChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);

                context.getToDelete().forEach((hello -> {
                    CatEventMonitor.DEFAULT.logEvent("Sentinel.Hello.Collector.Remove", hello.toString());
                    HostPort sentinelAddr = hello.getSentinelAddr();
                    deleteChain.add(sentinelManager.removeSentinelMonitor(new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort()), hello.getMonitorName()));
                }));

                deleteChain.execute().addListener(deleteFuture -> {
                    if (deleteFuture.isSuccess()) {
                        logger.info("[{}-{}][deleted]{}", LOG_TITLE, context.getSentinelMonitorName(), context.getToDelete());
                    } else {
                        logger.error("[{}-{}][deleted]{}", LOG_TITLE, context.getSentinelMonitorName(), context.getToDelete(), deleteFuture.cause());
                    }
                    future().setSuccess();
                });
            } else {
                logger.warn("[{}-{}][toDelete]no action due to master: {} lose slaves, current slaves: {}", LOG_TITLE, context.getSentinelMonitorName(),
                        context.getTrueMasterInfo().getKey(), context.getTrueMasterInfo().getValue());
                future().setFailure(new SentinelsException("to delete all sentinels when master lose slaves"));
            }
        }
    }

    boolean shouldDelete() {
        return !needCheckMasterSlaves || trueMasterHasAllSlaves() || !deleteAllSentinels();
    }

    boolean trueMasterHasAllSlaves() {
        Pair<HostPort, List<HostPort>> trueMasterInfo = context.getTrueMasterInfo();
        List<HostPort> shardSlaves = context.getShardInstances();
        shardSlaves.remove(trueMasterInfo.getKey());

        shardSlaves.removeAll(trueMasterInfo.getValue());
        return shardSlaves.isEmpty();
    }

    boolean deleteAllSentinels() {
        List<HostPort> toDeleteSentinels = context.getToDelete().stream().map(SentinelHello::getSentinelAddr).collect(Collectors.toList());

        Set<HostPort> allSentinels = context.getSentinels();
        allSentinels.removeAll(context.getNetworkErrorSentinels().keySet());
        allSentinels.removeAll(toDeleteSentinels);

        return allSentinels.isEmpty();
    }

}
