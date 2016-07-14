package com.ctrip.xpipe.redis.meta.server.job;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.retry.RetryTemplate;
import com.ctrip.xpipe.api.retry.RetryType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeObjectPoolFromKeyed;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractKeeperCommand.KeeperSetStateCommand;
import com.ctrip.xpipe.retry.RetryNTimes;

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
		
		if(activeKeeper == null){
			throw new IllegalStateException("no activekeeper " + keepers);
		}
		
		if(!executeKeeperState(activeKeeper, activeKeeperMaster)){
			future.setFailure(new Exception("set keeper to active fail:" + activeKeeper));
		}
		
		for(KeeperMeta keeperMeta : keepers){
			if(!keeperMeta.isActive()){
				executeKeeperState(keeperMeta, new InetSocketAddress(activeKeeper.getIp(), activeKeeper.getPort())); //backup fail ignore
			}
		}
		
		future.setSuccess(null);
	}

	private boolean executeKeeperState(final KeeperMeta keeper, final InetSocketAddress masterAddress) {

		RetryTemplate retryTemplate = new RetryNTimes(retryTimes, delayBaseMilli);
		
		return retryTemplate.execute(new Callable<RetryType>() {
			@Override
			public RetryType call() throws Exception {

				logger.info("[doExecute][change keeper to {} %s]", keeper, keeper.isActive() ? "active" : "backup");
				SimpleObjectPool<NettyClient> pool = new XpipeObjectPoolFromKeyed<InetSocketAddress, NettyClient>(clientPool, new InetSocketAddress(keeper.getIp(), keeper.getPort()));
				Boolean result = new KeeperSetStateCommand(pool, keeper.isActive() ? KeeperState.ACTIVE : KeeperState.BACKUP, masterAddress).execute().sync().get();
				if(!result){
					return RetryType.FAIL_RETRY;
				}
				return RetryType.SUCCESS;
			}
		});
	}

	@Override
	protected void doReset() throws InterruptedException, ExecutionException {
		throw new UnsupportedOperationException();
		
	}
}
