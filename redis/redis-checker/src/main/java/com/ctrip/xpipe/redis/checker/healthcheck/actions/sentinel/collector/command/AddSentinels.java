package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class AddSentinels extends AbstractSentinelHelloCollectCommand {

    private SentinelManager sentinelManager;
    private CheckerConfig checkerConfig;

    public AddSentinels(SentinelHelloCollectContext context, SentinelManager sentinelManager, CheckerConfig checkerConfig) {
        super(context);
        this.sentinelManager = sentinelManager;
        this.checkerConfig = checkerConfig;
    }

    @Override
    protected void doExecute() throws Throwable {
        if (context.getToAdd().size() == 0) {
            future().setSuccess();
        } else {
            logger.info("[{}-{}][toAdd]master: {}, stl: {}", LOG_TITLE, context.getSentinelMonitorName(), context.getTrueMasterInfo().getKey(),
                    context.getToAdd().stream().map(SentinelHello::getSentinelAddr).collect(Collectors.toSet()));

            ParallelCommandChain addChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
            context.getToAdd().forEach((hello) -> {
                CatEventMonitor.DEFAULT.logEvent("Sentinel.Hello.Collector.Add", hello.toString());
                HostPort sentinelAddr = hello.getSentinelAddr();
                Sentinel sentinel = new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort());
                addChain.add(sentinelManager.monitorMaster(sentinel, hello.getMonitorName(), hello.getMasterAddr(), checkerConfig.getDefaultSentinelQuorumConfig().getQuorum()));
            });

            addChain.execute().addListener(innerFuture -> {
                if (innerFuture.isSuccess()) {
                    logger.info("[{}-{}][added]{}", LOG_TITLE, context.getSentinelMonitorName(), context.getToAdd());
                } else {
                    logger.error("[{}-{}][added]{}", LOG_TITLE, context.getSentinelMonitorName(), context.getToAdd(), innerFuture.cause());
                }
                future().setSuccess();
            });
        }
    }

}
