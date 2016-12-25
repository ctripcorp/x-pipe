package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultPsync;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf.ReplConfType;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public class DefaultRedisMasterReplication extends AbstractRedisMasterReplication{
	
	private volatile PARTIAL_STATE partialState = PARTIAL_STATE.UNKNOWN;

	private ScheduledFuture<?> replConfFuture;
	
	protected int masterConnectRetryDelaySeconds = Integer.parseInt(System.getProperty(KEY_MASTER_CONNECT_RETRY_DELAY_SECONDS, "5"));

	public DefaultRedisMasterReplication(RedisMaster redisMaster, RedisKeeperServer redisKeeperServer, ScheduledExecutorService scheduled, int replTimeoutSeconds) {
		super(redisKeeperServer, redisMaster, scheduled, replTimeoutSeconds);
	}

	public DefaultRedisMasterReplication(RedisMaster redisMaster, RedisKeeperServer redisKeeperServer, ScheduledExecutorService scheduled) {
		this(redisMaster, redisKeeperServer, scheduled, DEFAULT_REPLICATION_TIMEOUT);
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
	public void masterDisconntected(Channel channel) {
		super.masterDisconntected(channel);
		
		long interval = System.currentTimeMillis() - connectedTime;
		long scheduleTime = masterConnectRetryDelaySeconds * 1000 - interval;
		if (scheduleTime < 0) {
			scheduleTime = 0;
		}
		scheduled.schedule(new Runnable() {

			@Override
			public void run() {
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
		
		if (replConfFuture != null) {
			replConfFuture.cancel(true);
			replConfFuture = null;
		}
	}

	private void scheduleReplconf() {

		if (logger.isInfoEnabled()) {
			logger.info("[scheduleReplconf]" + this);
		}
		
		if(replConfFuture != null){
			replConfFuture.cancel(true);
		}

		replConfFuture = scheduled.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				try {

					logger.debug("[run][send ack]{}", masterChannel);
					Command<Object> command = new Replconf(clientPool, ReplConfType.ACK, String.valueOf(redisMaster.getCurrentReplicationStore().getEndOffset()), scheduled);
					command.execute();
				} catch (Throwable th) {
					logger.error("[run][send replack error]" + DefaultRedisMasterReplication.this, th);
				}
			}
		}, REPLCONF_INTERVAL_MILLI, REPLCONF_INTERVAL_MILLI, TimeUnit.MILLISECONDS);
	}


	@Override
	protected void kinfoFail(Throwable e) {
		
		logger.info("[doWhenKinfoFail][retry]");
		scheduled.schedule(new Runnable() {

			@Override
			public void run() {
				try {
					executeCommand(kinfoCommand());
				} catch (CommandExecutionException e) {
					logger.error("[run]", e);
				}
			}
		}, 1, TimeUnit.SECONDS);
	}

	@Override
	protected void psyncFail(Throwable cause) {
		
		logger.info("[psyncFail][close channel, wait for reconnect]" + masterChannel, cause);
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
		
		if(redisKeeperServer.getRedisKeeperServerState().sendKinfo()){
			logger.info("[doBeginWriteRdb]{}", masterRdbOffset);
			saveKinfo();
		}
	}

	protected void saveKinfo(){
		try{
			redisMaster.getCurrentReplicationStore().getMetaStore().saveKinfo(getKinfo());
		} catch (IOException e) {
			throw new IllegalStateException("[saveKinfo][save kinfo]" + getKinfo());
		}
	}


	@Override
	protected void doEndWriteRdb() {
		scheduleReplconf();
		
		redisMaster.setMasterState(MASTER_STATE.REDIS_REPL_CONNECTED);
	}

	@Override
	protected void doOnContinue(){
		
		redisMaster.setMasterState(MASTER_STATE.REDIS_REPL_CONNECTED);
		try {
			redisMaster.getCurrentReplicationStore().getMetaStore().setMasterAddress((DefaultEndPoint) redisMaster.masterEndPoint());
		} catch (IOException e) {
			logger.error("[doOnContinue]" + this, e);
		}
		
		scheduleReplconf();
		partialState = PARTIAL_STATE.PARTIAL;
		redisKeeperServer.getRedisKeeperServerState().initPromotionState();
		
		if(redisKeeperServer.getRedisKeeperServerState().sendKinfo()){
			saveKinfo();
		}
	}

	@Override
	protected void doReFullSync() {
		redisKeeperServer.getRedisKeeperServerState().initPromotionState();
	}

	@Override
	protected void doOnFullSync() {
		
		try {
			logger.info("[doOnFullSync]{}", masterChannel);
			RdbDumper rdbDumper  = new RedisMasterReplicationRdbDumper(this, redisKeeperServer);
			setRdbDumper(rdbDumper);
			redisKeeperServer.setRdbDumper(rdbDumper, true);
		} catch (SetRdbDumperException e) {
			//impossible to happen
			logger.error("[doOnFullSync][impossible to happen]", e);
		}
	}
}
