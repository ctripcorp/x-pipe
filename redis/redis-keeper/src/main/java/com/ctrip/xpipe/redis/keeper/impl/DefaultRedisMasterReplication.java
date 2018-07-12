package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultPsync;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf.ReplConfType;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public class DefaultRedisMasterReplication extends AbstractRedisMasterReplication{
	
	private volatile PARTIAL_STATE partialState = PARTIAL_STATE.UNKNOWN;

	private ScheduledFuture<?> replConfFuture;

	public DefaultRedisMasterReplication(RedisMaster redisMaster, RedisKeeperServer redisKeeperServer,
										 NioEventLoopGroup nioEventLoopGroup, ScheduledExecutorService scheduled,
										 int replTimeoutMilli, ProxyResourceManager endpointManager) {
		super(redisKeeperServer, redisMaster, nioEventLoopGroup, scheduled, replTimeoutMilli, endpointManager);
	}

	public DefaultRedisMasterReplication(RedisMaster redisMaster, RedisKeeperServer redisKeeperServer,
										 NioEventLoopGroup nioEventLoopGroup, ScheduledExecutorService scheduled,
										 ProxyResourceManager endpointManager) {
		this(redisMaster, redisKeeperServer, nioEventLoopGroup, scheduled, DEFAULT_REPLICATION_TIMEOUT_MILLI,
				endpointManager);
	}

	@Override
	protected void doConnect(Bootstrap b) {
		
		redisMaster.setMasterState(MASTER_STATE.REDIS_REPL_CONNECTING);

		tryConnect(b).addListener(new ChannelFutureListener() {
			
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				
				if(!future.isSuccess()){
					
					logger.error("[operationComplete][fail connect with master]" + redisMaster, future.cause());
					
					scheduled.schedule(new Runnable() {
						@Override
						public void run() {
							try{
								connectWithMaster();
							}catch(Throwable th){
								logger.error("[run][connectUntilConnected]" + DefaultRedisMasterReplication.this, th);
							}
						}
					}, masterConnectRetryDelaySeconds, TimeUnit.SECONDS);
				}
			}
		});
	}
	
	
	@Override
	public void masterConnected(Channel channel) {
		
		redisMaster.setMasterState(MASTER_STATE.REDIS_REPL_HANDSHAKE);
		
		super.masterConnected(channel);
		cancelReplConf();
	}
	
	@Override
	public void masterDisconntected(Channel channel) {
		super.masterDisconntected(channel);
		
		long interval = System.currentTimeMillis() - connectedTime;
		long scheduleTime = masterConnectRetryDelaySeconds * 1000 - interval;
		if (scheduleTime < 0) {
			scheduleTime = 0;
		}
		logger.info("[masterDisconntected][reconnect after {} ms]", scheduleTime);
		scheduled.schedule(new AbstractExceptionLogTask() {

			@Override
			public void doRun() {
				connectWithMaster();
			}
		}, scheduleTime, TimeUnit.MILLISECONDS);
	}
	
	public void setMasterConnectRetryDelaySeconds(int masterConnectRetryDelaySeconds) {
		this.masterConnectRetryDelaySeconds = masterConnectRetryDelaySeconds;
	}

	@Override
	public void stopReplication() {
		super.stopReplication();

		cancelReplConf();
	}

	private void scheduleReplconf() {

		if (logger.isInfoEnabled()) {
			logger.info("[scheduleReplconf]" + this);
		}
		
		cancelReplConf();

		replConfFuture = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
			
			@Override
			protected void doRun() throws Exception {
				
				logger.debug("[run][send ack]{}", masterChannel);
				
				Command<Object> command = createReplConf();
				command.execute();
				
			}

		}, 0, REPLCONF_INTERVAL_MILLI, TimeUnit.MILLISECONDS);
	}

	protected void cancelReplConf() {
		
		if (replConfFuture != null) {
			replConfFuture.cancel(true);
			replConfFuture = null;
		}
	}
	
	protected Command<Object> createReplConf() {
		
		return new Replconf(clientPool, ReplConfType.ACK, scheduled, String.valueOf(redisMaster.getCurrentReplicationStore().getEndOffset()));
	}

	@Override
	protected void psyncFail(Throwable cause) {
		
		logger.info("[psyncFail][close channel, wait for reconnect]" + this, cause);
		masterChannel.close();
	}

	@Override
	protected Psync createPsync() {
		
		Psync psync = new DefaultPsync(clientPool, redisMaster.masterEndPoint(), redisMaster.getReplicationStoreManager(), scheduled);
		psync.addPsyncObserver(this);
		psync.addPsyncObserver(redisKeeperServer);
		return psync;
	}
	
	@Override
	public PARTIAL_STATE partialState() {
		return partialState;
	}


	@Override
	protected void doBeginWriteRdb(EofType eofType, long masterRdbOffset) throws IOException {

		redisMaster.setMasterState(MASTER_STATE.REDIS_REPL_TRANSFER);
		
		partialState = PARTIAL_STATE.FULL;
		redisMaster.getCurrentReplicationStore().getMetaStore().setMasterAddress((DefaultEndPoint) redisMaster.masterEndPoint());
	}

	@Override
	protected void doEndWriteRdb() {
		logger.info("[doEndWriteRdb]{}", this);
		redisMaster.setMasterState(MASTER_STATE.REDIS_REPL_CONNECTED);
		scheduleReplconf();
		
	}

	@Override
	protected void doOnContinue(){
		
		logger.info("[doOnContinue]{}", this);
		redisMaster.setMasterState(MASTER_STATE.REDIS_REPL_CONNECTED);
		try {
			redisMaster.getCurrentReplicationStore().getMetaStore().setMasterAddress((DefaultEndPoint) redisMaster.masterEndPoint());
		} catch (IOException e) {
			logger.error("[doOnContinue]" + this, e);
		}
		
		scheduleReplconf();
		partialState = PARTIAL_STATE.PARTIAL;
		redisKeeperServer.getRedisKeeperServerState().initPromotionState();
	}

	@Override
	protected void doReFullSync() {
		redisKeeperServer.getRedisKeeperServerState().initPromotionState();
	}

	@Override
	protected void doOnFullSync() {
		
		try {
			logger.info("[doOnFullSync]{}", this);
			RdbDumper rdbDumper  = new RedisMasterReplicationRdbDumper(this, redisKeeperServer);
			setRdbDumper(rdbDumper);
			redisKeeperServer.setRdbDumper(rdbDumper, true);
		} catch (SetRdbDumperException e) {
			//impossible to happen
			logger.error("[doOnFullSync][impossible to happen]", e);
		}
	}

	@Override
	protected String getSimpleName() {
		return "DefRep";
	}
}
