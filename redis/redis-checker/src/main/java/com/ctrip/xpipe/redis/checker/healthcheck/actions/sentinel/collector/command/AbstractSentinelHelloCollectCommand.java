package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.redis.core.protocal.pojo.SentinelMasterInstance;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketException;
import java.util.Set;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public abstract class AbstractSentinelHelloCollectCommand extends AbstractCommand<Void> {

    protected static Logger logger = LoggerFactory.getLogger("SentinelHelloCollectorCommand");

    protected SentinelHelloCollectContext context;

    public AbstractSentinelHelloCollectCommand(SentinelHelloCollectContext context) {
        this.context = context;
    }

    protected void collectAllMasters() {
        HostPort metaMaster = context.getMetaMaster();
        context.getProcessedHellos().forEach(sentinelHello -> context.getAllMasters().add(sentinelHello.getMasterAddr()));
        if (metaMaster != null)
            context.getAllMasters().add(metaMaster);
    }

    protected boolean currentMasterConsistent(Set<HostPort> currentMasters) {
        return currentMasters != null && currentMasters.size() == 1;
    }

    Command<SentinelMasterInstance> sentinelMaster(SentinelManager sentinelManager, HostPort sentinel) {
        Command<SentinelMasterInstance> command = sentinelManager.getMasterOfMonitor(new Sentinel(String.format("%s:%d", sentinel.getHost(), sentinel.getPort()), sentinel.getHost(), sentinel.getPort()), context.getSentinelMonitorName());
        command.future().addListener(innerFuture -> {
            if (innerFuture.isSuccess()) {
                context.getSentinelMonitors().put(sentinel, innerFuture.get());
                logger.info("[{}-{}+{}][getMasterOfMonitor]{},{},{}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), sentinel, context.getSentinelMonitorName(), innerFuture.get());
            } else {
                if (networkError(innerFuture.cause()))
                    context.getNetworkErrorSentinels().put(sentinel, innerFuture.cause());
                logger.warn("[{}-{}+{}][getMasterOfMonitor]{},{}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), sentinel, context.getSentinelMonitorName(), innerFuture.cause());
            }
        });
        return command;
    }

    boolean networkError(Throwable th) {
        return (th instanceof CommandTimeoutException) || (th instanceof SocketException);
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    protected void doReset() {

    }

    @VisibleForTesting
    void setContext(SentinelHelloCollectContext context) {
        this.context = context;
    }
}
