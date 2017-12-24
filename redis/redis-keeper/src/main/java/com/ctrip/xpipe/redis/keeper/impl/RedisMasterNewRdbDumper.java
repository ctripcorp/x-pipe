package com.ctrip.xpipe.redis.keeper.impl;


import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Aug 25, 2016
 */
public class RedisMasterNewRdbDumper extends AbstractRdbDumper{

	private DumpedRdbStore dumpedRdbStore;
	
	private RedisMaster redisMaster;
	
	private RdbonlyRedisMasterReplication rdbonlyRedisMasterReplication;

	private NioEventLoopGroup nioEventLoopGroup;
	
	private ScheduledExecutorService scheduled;

	public RedisMasterNewRdbDumper(RedisMaster redisMaster, RedisKeeperServer redisKeeperServer, NioEventLoopGroup nioEventLoopGroup, ScheduledExecutorService scheduled) {
		super(redisKeeperServer);
		this.redisMaster = redisMaster;
		this.nioEventLoopGroup = nioEventLoopGroup;
		this.scheduled = scheduled;
	}

	@Override
	protected void doExecute() throws Exception {
		
		rdbonlyRedisMasterReplication = new RdbonlyRedisMasterReplication(redisKeeperServer, redisMaster, nioEventLoopGroup, scheduled, this);
		
		rdbonlyRedisMasterReplication.initialize();
		rdbonlyRedisMasterReplication.start();
		
		future().addListener(new CommandFutureListener<Void>() {
			
			@Override
			public void operationComplete(CommandFuture<Void> commandFuture) throws Exception {
				releaseResource();
			}
		});
		
	}

	protected void releaseResource() {
		
		try{
			LifecycleHelper.stopIfPossible(rdbonlyRedisMasterReplication);
			LifecycleHelper.disposeIfPossible(rdbonlyRedisMasterReplication);
		}catch(Exception e){
			logger.error("[releaseResource]" + rdbonlyRedisMasterReplication, e);
		}
		
	}

	
	@Override
	protected void doCancel() {
		super.doCancel();
		
		logger.info("[doCancel][release resource]");
		releaseResource();
	}

	@Override
	public DumpedRdbStore prepareRdbStore() throws IOException {
		
		dumpedRdbStore = redisMaster.getCurrentReplicationStore().prepareNewRdb();
		logger.info("[prepareRdbStore]{}", dumpedRdbStore);
		return dumpedRdbStore;
	}
	
	@Override
	public void beginReceiveRdbData(long masterOffset) {
		
		try {
			logger.info("[beginReceiveRdbData][update rdb]{}", dumpedRdbStore);
			redisMaster.getCurrentReplicationStore().rdbUpdated(dumpedRdbStore);
			super.beginReceiveRdbData(masterOffset);
		} catch (IOException e) {
			logger.error("[beginReceiveRdbData]", e);
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s(%s)", getClass().getSimpleName(), rdbonlyRedisMasterReplication);
	}
}
