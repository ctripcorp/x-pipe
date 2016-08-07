package com.ctrip.xpipe.redis.meta.server.job;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
	private InetSocketAddress activeKeeperMaster;
	private SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool;
	private int delayBaseMilli = 1000;
	private int retryTimes = 5;
	
	public KeeperStateChangeJob(List<KeeperMeta> keepers, InetSocketAddress activeKeeperMaster, SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool){
		
		this.keepers = new LinkedList<>(keepers);
		this.activeKeeperMaster = activeKeeperMaster;
		this.clientPool = clientPool;
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

		SequenceCommandChain chain = new SequenceCommandChain(false);

		if(activeKeeper != null && activeKeeperMaster != null){
			Command<?> setActiveCommand = createKeeperSetStateCommand(activeKeeper, activeKeeperMaster);
			chain.add(setActiveCommand);
		}

		ParallelCommandChain backupChain = new ParallelCommandChain();
		
		for(KeeperMeta keeperMeta : keepers){
			if(!keeperMeta.isActive()){
				Command<?> backupCommand = createKeeperSetStateCommand(keeperMeta, new InetSocketAddress(activeKeeper.getIp(), activeKeeper.getPort()));
				backupChain.add(backupCommand);
			}
		}

		chain.add(backupChain);
		
		chain.execute().addListener(new CommandFutureListener<List<CommandFuture<?>>>() {
			
			@Override
			public void operationComplete(CommandFuture<List<CommandFuture<?>>> commandFuture) throws Exception {
				
				if(commandFuture.isSuccess()){
					future.setSuccess(null);
				}else{
					future.setFailure(commandFuture.cause());
				}
			}
		});;
	}

	private Command<?> createKeeperSetStateCommand(KeeperMeta keeper, InetSocketAddress masterAddress) {
		
		SimpleObjectPool<NettyClient> pool = new XpipeObjectPoolFromKeyed<InetSocketAddress, NettyClient>(clientPool, new InetSocketAddress(keeper.getIp(), keeper.getPort()));
		KeeperSetStateCommand command =  new KeeperSetStateCommand(pool, keeper.isActive() ? KeeperState.ACTIVE : KeeperState.BACKUP, masterAddress);
		return new CommandRetryWrapper<String>(retryTimes, new RetryDelay(delayBaseMilli), command);
	}

	@Override
	protected void doReset() throws InterruptedException, ExecutionException {
		throw new UnsupportedOperationException();
		
	}
	
}
