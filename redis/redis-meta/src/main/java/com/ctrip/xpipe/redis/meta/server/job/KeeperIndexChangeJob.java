package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.RequestResponseCommand;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.CommandRetryWrapper;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeObjectPoolFromKeyed;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperIndexState;
import com.ctrip.xpipe.retry.RetryDelay;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.core.protocal.cmd.AbstractKeeperCommand.*;
import static com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;

/**
 * @author ayq
 * <p>
 * 2022/11/24 16:38
 */
public class KeeperIndexChangeJob extends AbstractCommand<Void> implements RequestResponseCommand<Void> {

    private List<KeeperMeta> keepers;
    private KeeperIndexState indexState;
    private SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool;
    private ScheduledExecutorService scheduled;
    private Executor executors;

    private int delayBaseMilli;
    private int retryTimes;

    public KeeperIndexChangeJob(List<KeeperMeta> keepers,
                                KeeperIndexState indexState,
                                SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool,
                                ScheduledExecutorService scheduled, Executor executors) {
        this(keepers, indexState, clientPool, 1000, 5, scheduled, executors);
    }

    public KeeperIndexChangeJob(List<KeeperMeta> keepers,
                                KeeperIndexState indexState,
                                SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool,
                                int delayBaseMilli, int retryTimes, ScheduledExecutorService scheduled, Executor executors) {
        this.keepers = new LinkedList<>(keepers);
        this.indexState = indexState;
        this.clientPool = clientPool;
        this.delayBaseMilli = delayBaseMilli;
        this.retryTimes = retryTimes;
        this.scheduled = scheduled;
        this.executors = executors;
    }

    @Override
    public String getName() {
        return "keeper index change job";
    }

    @Override
    protected void doReset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return String.format("[%s] index: %s",
                StringUtil.join(",", (keeper) -> String.format("%s", keeper.desc()), keepers),
                indexState.name());
    }

    @Override
    public int getCommandTimeoutMilli() {
        return DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI * 2;
    }

    @Override
    protected void doExecute() throws Throwable {

        if (future().isDone()) {
            return;
        }

        ParallelCommandChain chain = new ParallelCommandChain(executors);
        for (KeeperMeta keeper : keepers) {
            Command<?> command = createKeeperSetIndexCommand(keeper, indexState);
            chain.add(command);
        }

        if (future().isDone()) {
            return;
        }

        chain.execute().addListener(commandFuture -> {
            if (commandFuture.isSuccess()) {
                future().setSuccess(null);
            } else {
                future().setFailure(commandFuture.cause());
            }
        });
    }

    private Command<?> createKeeperSetIndexCommand(KeeperMeta keeper, KeeperIndexState indexState) {

        SimpleObjectPool<NettyClient> pool = new XpipeObjectPoolFromKeyed<Endpoint, NettyClient>(clientPool, new DefaultEndPoint(keeper.getIp(), keeper.getPort()));

        KeeperSetIndexCommand command = new KeeperSetIndexCommand(pool, indexState, scheduled);

        return CommandRetryWrapper.buildCountRetry(retryTimes, new RetryDelay(delayBaseMilli), command, scheduled);
    }
}
