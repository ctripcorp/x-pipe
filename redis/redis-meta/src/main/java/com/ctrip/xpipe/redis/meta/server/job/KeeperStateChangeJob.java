package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.command.RequestResponseCommand;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.command.*;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeObjectPoolFromKeyed;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractKeeperCommand.KeeperSetStateCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.retry.RetryDelay;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

import static com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;

/**
 * @author wenchao.meng
 *
 * Jul 8, 2016
 */
public class KeeperStateChangeJob extends AbstractCommand<Void> implements RequestResponseCommand<Void> {
	
	private List<KeeperMeta> keepers;
	private Pair<String, Integer> activeKeeperMaster;
	private RouteMeta routeForActiveKeeper;
	private SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool;
	private int delayBaseMilli = 1000;
	private int retryTimes = 5;
	private ScheduledExecutorService scheduled;
	private Executor executors;
	private Command<?> activeSuccessCommand;

	public KeeperStateChangeJob(List<KeeperMeta> keepers,
								Pair<String, Integer> activeKeeperMaster,
								RouteMeta routeForActiveKeeper,
								SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool
			, ScheduledExecutorService scheduled, Executor executors){
		this(keepers, activeKeeperMaster, routeForActiveKeeper, clientPool, 1000, 5, scheduled, executors);
	}
	
	public KeeperStateChangeJob(List<KeeperMeta> keepers,
								Pair<String, Integer> activeKeeperMaster,
								RouteMeta routeForActiveKeeper,
								SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool
			, int delayBaseMilli, int retryTimes, ScheduledExecutorService scheduled, Executor executors){
		this.keepers = new LinkedList<>(keepers);
		this.activeKeeperMaster = activeKeeperMaster;
		this.routeForActiveKeeper = routeForActiveKeeper;
		this.clientPool = clientPool;
		this.delayBaseMilli = delayBaseMilli;
		this.retryTimes = retryTimes;
		this.scheduled = scheduled;
		this.executors = executors;
	}

	@Override
	public String getName() {
		return "keeper change job";
	}

	@Override
	protected void doExecute() throws CommandExecutionException {

		if(future().isDone()) {
			return;
		}
		KeeperMeta activeKeeper = null;
		for(KeeperMeta keeperMeta : keepers){
			if(keeperMeta.isActive()){
				activeKeeper = keeperMeta;
				break;
			}
		}

		if(activeKeeper == null){
			future().setFailure(new Exception("can not find active keeper:" + keepers));
			return;
		}
		SequenceCommandChain chain = new SequenceCommandChain(false);

		if(activeKeeperMaster != null){
			Command<?> setActiveCommand = createKeeperSetStateCommand(activeKeeper, activeKeeperMaster);
			addActiveCommandHook(setActiveCommand);
			chain.add(setActiveCommand);
		}

		ParallelCommandChain backupChain = new ParallelCommandChain(executors);
		
		for(KeeperMeta keeperMeta : keepers){
			if(!keeperMeta.isActive()){
				Command<?> backupCommand = createKeeperSetStateCommand(keeperMeta, new Pair<String, Integer>(activeKeeper.getIp(), activeKeeper.getPort()));
				backupChain.add(backupCommand);
			}
		}

		chain.add(backupChain);

		if(future().isDone()) {
			return;
		}
		chain.execute().addListener(new CommandFutureListener<Object>() {
			
			@Override
			public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
				
				if(commandFuture.isSuccess()){
					future().setSuccess(null);
				}else{
					future().setFailure(commandFuture.cause());
				}
			}
		});
	}

	private Command<?> createKeeperSetStateCommand(KeeperMeta keeper, Pair<String, Integer> masterAddress) {
		
		SimpleObjectPool<NettyClient> pool = new XpipeObjectPoolFromKeyed<Endpoint, NettyClient>(clientPool, new DefaultEndPoint(keeper.getIp(), keeper.getPort()));

		KeeperSetStateCommand command =  new KeeperSetStateCommand(pool,
				keeper.isActive() ? KeeperState.ACTIVE : KeeperState.BACKUP,
				masterAddress,
				keeper.isActive() ? routeForActiveKeeper : null,
				scheduled);
		return CommandRetryWrapper.buildCountRetry(retryTimes, new RetryDelay(delayBaseMilli), command, scheduled);
	}

	@Override
	protected void doReset(){
		throw new UnsupportedOperationException();
	}
	
	public void setActiveSuccessCommand(Command<?> activeSuccessCommand) {
		this.activeSuccessCommand = activeSuccessCommand;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void addActiveCommandHook(final Command<?> setActiveCommand) {
		
		setActiveCommand.future().addListener(new CommandFutureListener() {

			@Override
			public void operationComplete( CommandFuture commandFuture) throws Exception {
				
				if(commandFuture.isSuccess() && activeSuccessCommand != null){
					getLogger().info("[addActiveCommandHook][set active success, execute hook]{}, {}", setActiveCommand, activeSuccessCommand);
					activeSuccessCommand.execute();
				}
			}
		});
	}

	@Override
	public String toString() {
		return String.format("[%s] master: %s",
				StringUtil.join(",", (keeper) -> String.format("%s.%s", keeper.desc(), keeper.isActive()), keepers),
				activeKeeperMaster);
	}

	@Override
	public int getCommandTimeoutMilli() {
		return DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI * 2;
	}
}
