package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.command.RequestResponseCommand;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.*;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeObjectPoolFromKeyed;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.ApplierState;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractApplierCommand.ApplierSetStateCommand;
import com.ctrip.xpipe.retry.RetryDelay;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;

/**
 * @author ayq
 * <p>
 * 2022/4/11 15:26
 */
public class ApplierStateChangeJob extends AbstractCommand<Void> implements RequestResponseCommand<Void> {

    private List<ApplierMeta> appliers;

    private Pair<String, Integer> activeApplierMaster;

    private String sids;

    private GtidSet gtidSet;

    private RouteMeta routeForActiveApplier;

    private SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool;

    private int delayBaseMilli;

    private int retryTimes;

    private ScheduledExecutorService scheduled;

    private Executor executors;

    private Command<?> activeSuccessCommand;

    public ApplierStateChangeJob(List<ApplierMeta> appliers,
                                 Pair<String, Integer> activeApplierMaster,
                                 String sids,
                                 GtidSet gtidSet,
                                 RouteMeta routeForActiveApplier,
                                 SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool,
                                 ScheduledExecutorService scheduled, Executor executors) {
        this(appliers, activeApplierMaster, sids, gtidSet, routeForActiveApplier, clientPool, 1000, 5, scheduled, executors);
    }

    public ApplierStateChangeJob(List<ApplierMeta> appliers,
                                 Pair<String, Integer> activeApplierMaster,
                                 String sids,
                                 GtidSet gtidSet,
                                 RouteMeta routeForActiveApplier,
                                 SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool,
                                 int delayBaseMilli,
                                 int retryTimes,
                                 ScheduledExecutorService scheduled,
                                 Executor executors) {
        this.appliers = new LinkedList<>(appliers);
        this.activeApplierMaster = activeApplierMaster;
        this.sids = sids;
        this.gtidSet = gtidSet;
        this.routeForActiveApplier = routeForActiveApplier;
        this.clientPool = clientPool;
        this.delayBaseMilli = delayBaseMilli;
        this.retryTimes = retryTimes;
        this.scheduled = scheduled;
        this.executors = executors;
    }

    @Override
    public String getName() {
        return "applier change job";
    }

    @Override
    protected void doExecute() throws CommandExecutionException {

        if (future().isDone()) {
            return;
        }
        ApplierMeta activeApplier = null;
        for (ApplierMeta applierMeta : appliers) {
            if (applierMeta.isActive()) {
                activeApplier = applierMeta;
                break;
            }
        }

        if (activeApplier == null) {
            future().setFailure(new Exception("can not find active applier:" + appliers));
            return;
        }

        if (gtidSet == null || sids == null || sids.isEmpty()) {
            future().setFailure(new Exception("gtidSet or sid null or empty, gtidSet:" + gtidSet + " sids:" + sids));
            return;
        }

        SequenceCommandChain chain = new SequenceCommandChain(false);

        if (activeApplierMaster != null) {
            Command<?> setActiveCommand = createApplierSetStateCommand(activeApplier, activeApplierMaster, sids, gtidSet.toString());
            addActiveCommandHook(setActiveCommand);
            chain.add(setActiveCommand);
        }

        ParallelCommandChain backupChain = new ParallelCommandChain(executors);

        for (ApplierMeta applierMeta : appliers) {
            if (!applierMeta.isActive()) {
                Command<?> backupCommand = createApplierSetStateCommand(applierMeta,
                        new Pair<String, Integer>(activeApplier.getIp(), activeApplier.getPort()), sids, gtidSet.toString());
                backupChain.add(backupCommand);
            }
        }

        chain.add(backupChain);

        if (future().isDone()) {
            return;
        }
        chain.execute().addListener(new CommandFutureListener<Object>() {

            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {

                if (commandFuture.isSuccess()) {
                    future().setSuccess(null);
                } else {
                    future().setFailure(commandFuture.cause());
                }
            }
        });
    }

    private Command<?> createApplierSetStateCommand(ApplierMeta applier, Pair<String, Integer> masterAddress,
                                                    String sids, String gtidSet) {

        SimpleObjectPool<NettyClient> pool = new XpipeObjectPoolFromKeyed<Endpoint, NettyClient>(clientPool, new DefaultEndPoint(applier.getIp(), applier.getPort()));

        ApplierSetStateCommand command = new ApplierSetStateCommand(pool,
                applier.isActive() ? ApplierState.ACTIVE : ApplierState.BACKUP,
                masterAddress,
                sids,
                gtidSet,
                applier.isActive() ? routeForActiveApplier : null,
                scheduled);
        return CommandRetryWrapper.buildCountRetry(retryTimes, new RetryDelay(delayBaseMilli), command, scheduled);
    }

    @Override
    protected void doReset() {
        throw new UnsupportedOperationException();
    }

    public void setActiveSuccessCommand(Command<?> activeSuccessCommand) {
        this.activeSuccessCommand = activeSuccessCommand;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void addActiveCommandHook(final Command<?> setActiveCommand) {

        setActiveCommand.future().addListener(new CommandFutureListener() {

            @Override
            public void operationComplete(CommandFuture commandFuture) throws Exception {

                if (commandFuture.isSuccess() && activeSuccessCommand != null) {
                    getLogger().info("[addActiveCommandHook][set active success, execute hook]{}, {}", setActiveCommand, activeSuccessCommand);
                    activeSuccessCommand.execute();
                }
            }
        });
    }

    @Override
    public String toString() {
        return String.format("[%s] master: %s",
                StringUtil.join(",", (applier) -> String.format("%s.%s", applier.desc(), applier.isActive()), appliers),
                activeApplierMaster);
    }

    @Override
    public int getCommandTimeoutMilli() {
        return DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI * 2;
    }
}
