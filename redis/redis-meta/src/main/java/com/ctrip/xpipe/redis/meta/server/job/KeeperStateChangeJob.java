package com.ctrip.xpipe.redis.meta.server.job;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.unidal.tuple.Pair;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.command.CommandRetryWrapper;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeObjectPoolFromKeyed;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractKeeperCommand.KeeperSetStateCommand;
import com.ctrip.xpipe.retry.RetryDelay;

/**
 * @author wenchao.meng
 *
 * Jul 8, 2016
 */
public class KeeperStateChangeJob extends AbstractCommand<Void>{
	
	private List<KeeperMeta> keepers;
	private Pair<String, Integer> activeKeeperMaster;
	private SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool;
	private int delayBaseMilli = 1000;
	private int retryTimes = 5;
	private ScheduledExecutorService scheduled;
	private Executor executors;
	private Command<?> activeSuccessCommand;

	public KeeperStateChangeJob(List<KeeperMeta> keepers, Pair<String, Integer> activeKeeperMaster, SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool
			, ScheduledExecutorService scheduled, Executor executors){
		this(keepers, activeKeeperMaster, clientPool, 1000, 5, scheduled, executors);
	}
	
	public KeeperStateChangeJob(List<KeeperMeta> keepers, Pair<String, Integer> activeKeeperMaster, SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool
			, int delayBaseMilli, int retryTimes, ScheduledExecutorService scheduled, Executor executors){
		this.keepers = new LinkedList<>(keepers);
		this.activeKeeperMaster = activeKeeperMaster;
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

		KeeperMeta activeKeeper = null;
		for(KeeperMeta keeperMeta : keepers){
			if(keeperMeta.isActive()){
				activeKeeper = keeperMeta;
				break;
			}
		}

		if(activeKeeper == null){
			future().setFailure(new Exception("can not find active keeper:" + keepers));
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
		
		SimpleObjectPool<NettyClient> pool = new XpipeObjectPoolFromKeyed<InetSocketAddress, NettyClient>(clientPool, new InetSocketAddress(keeper.getIp(), keeper.getPort()));
		KeeperSetStateCommand command =  new KeeperSetStateCommand(pool, keeper.isActive() ? KeeperState.ACTIVE : KeeperState.BACKUP, masterAddress, scheduled);
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
					logger.info("[addActiveCommandHook][set active success, execute hook]{}", setActiveCommand, activeSuccessCommand);
					activeSuccessCommand.execute();
				}
			}
		});
	}
}
