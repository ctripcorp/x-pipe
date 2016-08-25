package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;

/**
 * @author wenchao.meng
 *
 * Aug 25, 2016
 */
public class RedisMasterNewRdbDumper extends AbstractRdbDumper{

	private File rdbFile;
	
	private RedisMaster redisMaster;
	
	private RdbonlyRedisMasterReplication rdbonlyRedisMasterReplication;

	public RedisMasterNewRdbDumper(RedisMaster redisMaster, RedisKeeperServer redisKeeperServer) {
		super(redisKeeperServer);
		this.redisMaster = redisMaster;
	}

	@Override
	protected void doExecute() throws Exception {
		
		rdbonlyRedisMasterReplication = new RdbonlyRedisMasterReplication(redisKeeperServer, redisMaster, this);
		
		rdbonlyRedisMasterReplication.initialize();
		rdbonlyRedisMasterReplication.start();
		
		future.addListener(new CommandFutureListener<Void>() {
			
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
	protected void doReset() throws InterruptedException, ExecutionException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	protected void doCancel() {
		super.doCancel();
		
		logger.info("[doCancel][release resource]");
		releaseResource();
	}

	@Override
	public File prepareRdbFile() {
		
		rdbFile = redisMaster.getCurrentReplicationStore().prepareNewRdbFile();
		logger.info("[prepareRdbFile]{}", rdbFile);
		return rdbFile;
	}

	@Override
	public void beginReceiveRdbData(long masterOffset) {
		try {
			redisMaster.getCurrentReplicationStore().rdbUpdated(rdbFile.getName(), masterOffset);
			super.beginReceiveRdbData(masterOffset);
		} catch (IOException e) {
			logger.error("[waitUntilPsyncDone]", e);
		}
	}
}
