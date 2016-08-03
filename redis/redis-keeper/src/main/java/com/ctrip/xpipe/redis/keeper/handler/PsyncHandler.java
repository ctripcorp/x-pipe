package com.ctrip.xpipe.redis.keeper.handler;



import java.io.IOException;

import com.ctrip.xpipe.redis.core.protocal.cmd.Psync;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:49:02
 */
public class PsyncHandler extends AbstractCommandHandler{
	
	public static final int WAIT_OFFSET_TIME_MILLI = 60 * 1000;
	
	@Override
	protected void doHandle(final String[] args, final RedisClient redisClient) {
		// in non-psync executor
		final RedisKeeperServer redisKeeperServer = redisClient.getRedisKeeperServer();
		
		if(!redisKeeperServer.getRedisKeeperServerState().psync(redisClient, args)){
			return;
		}
		
		final RedisSlave redisSlave  = redisClient.becomeSlave();
		if(redisSlave == null){
			logger.warn("[doHandle][psync client already slave]" + redisClient);
			try {
				redisClient.close();
			} catch (IOException e) {
				logger.error("[doHandle]" + redisClient, e);
			}
			return;
		}
		
		// transfer to psync executor, which will do psync dedicatedly
		redisSlave.processPsyncSequentially(new Runnable() {
			
			@Override
			public void run() {
				innerDoHandle(args, redisSlave, redisKeeperServer);
			}
		});
	}
	
	private void innerDoHandle(final String[] args, final RedisSlave redisSlave, RedisKeeperServer redisKeeperServer) {
		if(args[0].equals("?")){
			doFullSync(redisSlave);
		}else if(args[0].equals(redisKeeperServer.getKeeperRunid())){
			
			KeeperRepl keeperRepl = redisKeeperServer.getKeeperRepl();
			Long beginOffset = keeperRepl.getBeginOffset();
			Long endOffset = keeperRepl.getEndOffset();
			Long offsetRequest = Long.valueOf(args[1]);
			
			if(offsetRequest < beginOffset){
				logger.info("[doHandle][offset < beginOffset][begin, end, request]{}, {}, {}" , beginOffset , endOffset);
				doFullSync(redisSlave);
			}else if(offsetRequest > endOffset +1){
				
				logger.info("[run][wait for offset]{}, {} > {} + 1", redisSlave, offsetRequest, endOffset);
				waitForoffset(args, redisSlave, offsetRequest);
			}else{
				// TODO
				long maxOffsetLagBeforeFullSync = 10;
				if(endOffset - offsetRequest < maxOffsetLagBeforeFullSync) {
					doPartialSync(redisSlave, offsetRequest);
				} else {
					doFullSync(redisSlave);
				}
			}
		}else{
			doFullSync(redisSlave);
		}
	}

	/**
	 * wait until the request offset received
	 * @param redisClient 
	 * @param args 
	 * @param offset
	 */
	private void waitForoffset(final String[] args, final RedisSlave redisSlave, final Long offsetRequest) {
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				
				try {
					ReplicationStore replicationStore = redisSlave.getRedisKeeperServer().getReplicationStore();
					boolean result = replicationStore.getCommandStore().awaitCommandsOffset(offsetRequest - replicationStore.getMetaStore().getKeeperBeginOffset() - 1, WAIT_OFFSET_TIME_MILLI);
					if(result){
						logger.info("[waitForoffset][wait succeed]{}", redisSlave);
						doPartialSync(redisSlave, offsetRequest);
						return;
					}
				} catch (InterruptedException e) {
				}catch(Exception e){
					logger.error("[waitForoffset][failed]", e);
					try {
						redisSlave.close();
					} catch (IOException e1) {
						logger.error("[waitForoffset][close slave]" + redisSlave, e);
					}
					return;
				}
				logger.info("[run][offset wait failed]{}", redisSlave);
				doFullSync(redisSlave);
			}
		}).start();;
	}

	private void doPartialSync(RedisSlave redisSlave, Long offset) {
		
		if(logger.isInfoEnabled()){
			logger.info("[doPartialSync]" + redisSlave);
		}
		SimpleStringParser simpleStringParser = new SimpleStringParser(Psync.PARTIAL_SYNC);
		redisSlave.sendMessage(simpleStringParser.format());
		redisSlave.beginWriteCommands(offset);
		redisSlave.partialSync();
	}

	private void doFullSync(RedisSlave redisSlave) {

		try {
			if(logger.isInfoEnabled()){
				logger.info("[doFullSync]" + redisSlave);
			}
			RedisKeeperServer redisKeeperServer = redisSlave.getRedisKeeperServer();
			redisKeeperServer.fullSyncToSlave(redisSlave);
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
}
