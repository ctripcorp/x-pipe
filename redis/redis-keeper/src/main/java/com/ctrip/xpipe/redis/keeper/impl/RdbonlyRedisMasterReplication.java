package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.cmd.RdbOnlyPsync;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;
import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.store.RdbOnlyReplicationStore;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public class RdbonlyRedisMasterReplication extends AbstractRedisMasterReplication{

	private RdbOnlyReplicationStore rdbOnlyReplicationStore;
	private DumpedRdbStore dumpedRdbStore;
	
	public RdbonlyRedisMasterReplication(RedisKeeperServer redisKeeperServer, RedisMaster redisMaster,
                                         NioEventLoopGroup nioEventLoopGroup, ScheduledExecutorService scheduled,
                                         RdbDumper rdbDumper, ProxyResourceManager endpointManager) {
		super(redisKeeperServer, redisMaster, nioEventLoopGroup, scheduled, endpointManager);
		setRdbDumper(rdbDumper);
	}
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		
		dumpedRdbStore = getRdbDumper().prepareRdbStore();
		logger.info("[doInitialize][newRdbFile]{}", dumpedRdbStore);
		rdbOnlyReplicationStore = new RdbOnlyReplicationStore(dumpedRdbStore);
		
	}


	@Override
	protected void doConnect(Bootstrap b) {

		tryConnect(b).addListener(new ChannelFutureListener() {
			
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if(!future.isSuccess()){
					logger.info("[operationComplete][fail]", future.cause());
					dumpFail(future.cause());
				}
			}
		});
	}

	@Override
	protected void psyncFail(Throwable cause) {
	}

	@Override
	protected Psync createPsync() {
		
		Psync psync = new RdbOnlyPsync(clientPool, rdbOnlyReplicationStore, scheduled);
		psync.addPsyncObserver(this);
		return psync;
	}

	@Override
	public PARTIAL_STATE partialState() {
		return PARTIAL_STATE.FULL;
	}

	@Override
	protected void doReFullSync() {
		throw new IllegalStateException("impossible to be here");
	}

	@Override
	protected void doBeginWriteRdb(EofType eofType, long masterRdbOffset) throws IOException {
	}

	@Override
	protected void doEndWriteRdb() {
		
		logger.info("[endWriteRdb]{}", this);
		masterChannel.close();
	}

	@Override
	protected void doOnContinue() {
		throw new IllegalStateException("impossible to be here");
	}

	@Override
	protected void doOnFullSync() {
		
	}

	@Override
	protected String getSimpleName() {
		return "RdbRep";
	}
}
