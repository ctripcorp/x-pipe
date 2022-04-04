package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.google.common.util.concurrent.MoreExecutors;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class DeleteSentinels extends AbstractSentinelHelloCollectCommand {

    private SentinelManager sentinelManager;

    public DeleteSentinels(SentinelHelloCollectContext context, SentinelManager sentinelManager) {
        super(context);
        this.sentinelManager = sentinelManager;
    }

    @Override
    protected void doExecute() throws Throwable {

        if (context.getToDelete().size() == 0) {
            future().setSuccess();
        } else {
            logger.info("[{}-{}][toDelete]{}", LOG_TITLE, context.getSentinelMonitorName(), context.getToDelete());

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
        }

    }

}
