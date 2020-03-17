package com.ctrip.xpipe.redis.keeper.impl;


import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Aug 25, 2016
 */
public class RedisMasterNewRdbDumper extends AbstractRdbDumper{

	private Logger logger = LoggerFactory.getLogger(RedisMasterNewRdbDumper.class);

	private DumpedRdbStore dumpedRdbStore;
	
	private RedisMaster redisMaster;
	
	private RdbonlyRedisMasterReplication rdbonlyRedisMasterReplication;

	private NioEventLoopGroup nioEventLoopGroup;
	
	private ScheduledExecutorService scheduled;

	private KeeperResourceManager resourceManager;

	public RedisMasterNewRdbDumper(RedisMaster redisMaster, RedisKeeperServer redisKeeperServer,
                                   NioEventLoopGroup nioEventLoopGroup, ScheduledExecutorService scheduled,
                                   KeeperResourceManager resourceManager) {
		super(redisKeeperServer);
		this.redisMaster = redisMaster;
		this.nioEventLoopGroup = nioEventLoopGroup;
		this.scheduled = scheduled;
		this.resourceManager = resourceManager;
	}

	@Override
	protected void doExecute() throws Exception {
		
		rdbonlyRedisMasterReplication = new RdbonlyRedisMasterReplication(redisKeeperServer, redisMaster, nioEventLoopGroup, scheduled, this, resourceManager);
		
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

	@Override
	public Logger getLogger() {
		return logger;
	}
}
