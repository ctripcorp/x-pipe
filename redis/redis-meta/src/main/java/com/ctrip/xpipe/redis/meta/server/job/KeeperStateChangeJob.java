package com.ctrip.xpipe.redis.meta.server.job;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeObjectPoolFromKeyed;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractKeeperCommand.KeeperSetStateCommand;

/**
 * @author wenchao.meng
 *
 * Jul 8, 2016
 */
public class KeeperStateChangeJob extends AbstractCommand<Void>{
	
	private List<KeeperMeta> keepers;
	private InetSocketAddress activeKeeperMaster;
	private SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool;
	
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
	protected CommandFuture<Void> doExecute() throws CommandExecutionException {

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
		
		//change acitve keeper
		try {
			logger.info("[doExecute][change keeper to active]", activeKeeper);
			SimpleObjectPool<NettyClient> pool = new XpipeObjectPoolFromKeyed<InetSocketAddress, NettyClient>(clientPool, new InetSocketAddress(activeKeeper.getIp(), activeKeeper.getPort()));
			Boolean result = new KeeperSetStateCommand(pool, KeeperState.ACTIVE, activeKeeperMaster).execute().sync().get();
			if(!result){
				future.setFailure(new Exception("set keeperstate command result failure"));
				return future;
			}
		} catch (InterruptedException | ExecutionException e) {
			logger.error("[doExecute][change keeper to active fail]" + activeKeeper, e);
			future.setFailure(new Exception("change keeper to active fail", e));
			return future;
		}
		
		//change backup keeper
		for(KeeperMeta keeperMeta : keepers){
			if(!keeperMeta.isActive()){
				try {
					logger.info("[doExecute][change keeper to backup]", keeperMeta);
					SimpleObjectPool<NettyClient> pool = new XpipeObjectPoolFromKeyed<InetSocketAddress, NettyClient>(clientPool, new InetSocketAddress(keeperMeta.getIp(), keeperMeta.getPort()));
					new KeeperSetStateCommand(pool, KeeperState.BACKUP, new InetSocketAddress(activeKeeper.getIp(), activeKeeper.getPort())).execute().get();
				} catch (InterruptedException | ExecutionException e) {
					logger.error("[doExecute][change keeper to back fail]", e);
				}
			}
		}
		
		future.setSuccess(null);
		return future;
	}

}
