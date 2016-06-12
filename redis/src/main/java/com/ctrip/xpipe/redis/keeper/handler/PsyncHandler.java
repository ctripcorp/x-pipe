package com.ctrip.xpipe.redis.keeper.handler;


import java.io.IOException;

import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
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
		
		if(!redisKeeperServer.getRedisKeeperServerState().psync(redisClient, args)){
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
}
