package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.google.common.util.concurrent.MoreExecutors;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class SetSentinels extends AbstractSentinelHelloCollectCommand {

    private SentinelManager sentinelManager;

    public SetSentinels(SentinelHelloCollectContext context, SentinelManager sentinelManager) {
        super(context);
        this.sentinelManager = sentinelManager;
    }

    @Override
    protected void doExecute() throws Throwable {
        if (context.getToAdd().size() == 0) {
            future().setSuccess();
        } else {
            String[] sentinelConfigs = context.getClusterTypeSentinelConfig().get(context.getInfo().getClusterType());

            if (sentinelConfigs == null || sentinelConfigs.length == 0) {
                future().setSuccess();
            } else {
                ParallelCommandChain setChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
                context.getToAdd().forEach((hello) -> {
                    CatEventMonitor.DEFAULT.logEvent("Sentinel.Hello.Collector.SentinelSet", hello.getMonitorName());
                    HostPort sentinelAddr = hello.getSentinelAddr();
                    Sentinel sentinel = new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort());
                    setChain.add(sentinelManager.sentinelSet(sentinel, context.getSentinelMonitorName(), sentinelConfigs));
                });
                setChain.execute().addListener(innerFuture -> {
                    if (innerFuture.isSuccess()) {
                        logger.info("[{}-{}][sentinelSet]{},{}", LOG_TITLE, context.getSentinelMonitorName(), context.getToAdd(), sentinelConfigs);
                    } else {
                        logger.error("[{}-{}][sentinelSet]{},{}", LOG_TITLE, context.getSentinelMonitorName(), context.getToAdd(), sentinelConfigs, innerFuture.cause());
                    }
                    future().setSuccess();
                });
            }
        }
    }

    @Override
    protected void doReset() {}
}
