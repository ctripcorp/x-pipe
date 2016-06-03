package com.ctrip.xpipe.redis.keeper.handler;


import java.io.IOException;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer.ClusterRoleChanged;
import com.ctrip.xpipe.redis.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.protocal.cmd.Psync;
import com.ctrip.xpipe.redis.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.keeper.store.DefaultRdbFileListener;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:49:02
 */
public class PsyncHandler extends AbstractCommandHandler{
	
	@Override
	protected void doHandle(final String[] args, final RedisClient redisClient) {
		
		RedisKeeperServer redisKeeperServer = redisClient.getRedisKeeperServer();
		boolean shouldContinue = false;
		
		//TODO  backup slower than redis slave, try wait...
		switch(redisKeeperServer.getClusterRole()){
			case BACKUP:
				logger.error("[doHandle][server state backup, ask slave to wait.]" + redisKeeperServer.getClusterRole());
				redisClient.sendMessage(RedisProtocol.CRLF.getBytes());
				redisKeeperServer.addObserver(new PsyncClusterStateObserver(args, redisClient));
				break;
			case UNKNOWN:
				logger.error("[doHandle][server state unknown, close connection.]" + redisKeeperServer.getClusterRole());
				try {
					redisClient.close();
				} catch (IOException e) {
					logger.error("[doHandle][close redisClient]" + redisClient, e);
				}
				break;
			case ACTIVE:
				shouldContinue = true;
				break;
			default:
				throw new IllegalStateException("cluster role ilegal:" + redisKeeperServer.getClusterRole());
		}
		
		if(!shouldContinue){
			return;
		}
		
		RedisSlave redisSlave  = redisClient.becomeSlave();
		if(redisSlave == null){
			logger.warn("[doHandle][psync client already slave]" + redisClient);
			try {
				redisClient.close();
			} catch (IOException e) {
				logger.error("[doHandle]" + redisClient, e);
			}
			return;
		}
		
		
		if(args[0].equals("?")){
			doFullSync(redisSlave);
		}else if(args[0].equals(redisKeeperServer.getKeeperRunid())){
			
			KeeperRepl keeperRepl = redisKeeperServer.getKeeperRepl();
			Long beginOffset = keeperRepl.getBeginOffset();
			Long endOffset = keeperRepl.getEndOffset();
			Long offset = Long.valueOf(args[1]);
			
			if((offset > (endOffset + 1)) || (offset < beginOffset)){
				if(logger.isInfoEnabled()){
					logger.info("[doHandle][offset out of range, do FullSync]" + beginOffset + "," + endOffset + "," + offset);
				}
				doFullSync(redisSlave);
			}else{
				doPartialSync(redisSlave, offset);
			}
		}else{
			doFullSync(redisSlave);
		}
	}

	private void doPartialSync(RedisSlave redisSlave, Long offset) {
		
		if(logger.isInfoEnabled()){
			logger.info("[doPartialSync]" + redisSlave);
		}
		
		SimpleStringParser simpleStringParser = new SimpleStringParser(Psync.PARTIAL_SYNC);
		redisSlave.sendMessage(simpleStringParser.format());
		redisSlave.beginWriteCommands(offset);
	}

	private void doFullSync(RedisSlave redisSlave) {

		try {
			if(logger.isInfoEnabled()){
				logger.info("[doFullSync]" + redisSlave);
			}
			RedisKeeperServer redisKeeperServer = redisSlave.getRedisKeeperServer();
			redisKeeperServer.readRdbFile(new DefaultRdbFileListener(redisSlave));
		} catch (IOException e) {
			logger.error("[doFullSync][close client]" + redisSlave, e);
			try {
				redisSlave.close();
			} catch (IOException e1) {
				logger.error("[doFullSync]" + redisSlave, e1);
			}
		}
	}

	@Override
	public String[] getCommands() {
		
		return new String[]{"psync", "sync"};
	}
	
	class PsyncClusterStateObserver implements Observer, Releasable{
		
		private String []args;
		private RedisClient redisClient;
		
		public PsyncClusterStateObserver(String []args, RedisClient redisClient) {
			
			this.args = args;
			this.redisClient = redisClient;
		}
		
		@Override
		public void update(Object updateArgs, Observable observable) {
			
			logger.info("[update]{},{},{}", redisClient, updateArgs, observable);
			
			if(updateArgs instanceof ClusterRoleChanged){
				ClusterRoleChanged changed = (ClusterRoleChanged) updateArgs;
				switch(changed.getCurrent()){
					case ACTIVE:
						try {
							PsyncHandler.this.doHandle(args, redisClient);
							release();
						} catch (Exception e1) {
							logger.error("[update]" + redisClient + "," + changed, e1);
						}
						break;
					case BACKUP:
						break;
					case UNKNOWN:
					default:
						try {
							redisClient.close();
						} catch (IOException e) {
							logger.error("[state changed][wrong state, close client]" + changed, e);
						}
						throw new IllegalStateException();
				}
			}
			
		}

		@Override
		public void release() throws Exception {
			this.redisClient.getRedisKeeperServer().remoteObserver(this);
		}
		
	}
}
