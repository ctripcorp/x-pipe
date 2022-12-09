package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.redis.core.protocal.CAPA;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultPsync;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.core.store.OffsetReplicationProgress;
import com.ctrip.xpipe.redis.keeper.handler.keeper.AbstractSyncCommandHandler;

import java.io.IOException;

import static com.ctrip.xpipe.redis.core.protocal.Psync.KEEPER_PARTIAL_SYNC_OFFSET;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:49:02
 */
public class PsyncHandler extends AbstractSyncCommandHandler {
	
	public static final int WAIT_OFFSET_TIME_MILLI = 60 * 1000;

	@Override
	protected RedisSlave becomeSlave(RedisClient<?> redisClient) {
		return redisClient.becomeSlave();
	}

	protected void innerDoHandle(final String[] args, final RedisSlave redisSlave, RedisKeeperServer redisKeeperServer) {
		
		KeeperConfig keeperConfig = redisKeeperServer.getKeeperConfig();
		KeeperRepl keeperRepl = redisKeeperServer.getKeeperRepl();
		
		Long 	   offsetRequest = Long.valueOf(args[1]);
		String 	   replIdRequest = args[0];
		
		if(replIdRequest.equals("?")){
			if (redisSlave.isKeeper() && offsetRequest.equals(KEEPER_PARTIAL_SYNC_OFFSET) && null != keeperRepl.replId()) {
				logger.info("[innerDoHandler][keeper psync]");
				long continueOffset = keeperRepl.getEndOffset() + 1; // continue from next byte
				doKeeperPartialSync(redisSlave, keeperRepl.replId(), continueOffset);
			} else {
				doFullSync(redisSlave);
			}
		}else if(replIdRequest.equals(keeperRepl.replId()) || (replIdRequest.equals(keeperRepl.replId2()) && offsetRequest <= keeperRepl.secondReplIdOffset())){
			
			Long beginOffset = keeperRepl.getBeginOffset();
			Long endOffset = keeperRepl.getEndOffset();
			
			if(offsetRequest < beginOffset){
				logger.info("[innerDoHandle][offset < beginOffset][begin, end, request]{}, {}, {}" , beginOffset , endOffset, offsetRequest);
				((RedisKeeperServer)redisSlave.getRedisServer()).getKeeperMonitor().getKeeperStats().increatePartialSyncError();
				doFullSync(redisSlave);
			}else if(offsetRequest > endOffset +1){
				logger.info("[innerDoHandle][wait for offset]{}, {} > {} + 1", redisSlave, offsetRequest, endOffset);
				waitForoffset(args, redisSlave, keeperRepl.replId(), offsetRequest);
			}else{
				if(endOffset - offsetRequest < keeperConfig.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb()) {
					logger.info("[innerDoHandle][do partial sync]{}, {} - {} < {}", redisSlave, endOffset, offsetRequest, keeperConfig.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb());
					doPartialSync(redisSlave, keeperRepl.replId(), offsetRequest);
				} else {
					logger.info("[innerDoHandle][too much commands to transfer]{} - {} < {}", endOffset, offsetRequest, keeperConfig.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb());
					redisSlave.getRedisServer().getKeeperMonitor().getKeeperStats().increatePartialSyncError();
					doFullSync(redisSlave);
				}
			}
		}else{
			logger.info("current repl info: {}", keeperRepl);
			((RedisKeeperServer)redisSlave.getRedisServer()).getKeeperMonitor().getKeeperStats().increatePartialSyncError();
			doFullSync(redisSlave);
		}
	}

	protected void waitForoffset(final String[] args, final RedisSlave redisSlave, String replId, final Long offsetRequest) {

		try {
			ReplicationStore replicationStore = ((RedisKeeperServer)redisSlave.getRedisServer()).getReplicationStore();

			logger.info("[waitForoffset][begin wait]{}", redisSlave);
			boolean result = replicationStore.awaitCommandsOffset(offsetRequest - replicationStore.beginOffsetWhenCreated() - 1, WAIT_OFFSET_TIME_MILLI);
			if(result){
				logger.info("[waitForoffset][wait succeed]{}", redisSlave);
				((RedisKeeperServer)redisSlave.getRedisServer()).getKeeperMonitor().getKeeperStats().increaseWaitOffsetSucceed();
				doPartialSync(redisSlave, replId, offsetRequest);
				return;
			}
		} catch (InterruptedException e) {
			logger.error("[waitForoffset]" + redisSlave, e);
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
		((RedisKeeperServer)redisSlave.getRedisServer()).getKeeperMonitor().getKeeperStats().increasWaitOffsetFail();
		((RedisKeeperServer)redisSlave.getRedisServer()).getKeeperMonitor().getKeeperStats().increatePartialSyncError();
		doFullSync(redisSlave);
	}

	protected void doPartialSync(RedisSlave redisSlave, String replId, Long offset) {
		
		if(logger.isInfoEnabled()){
			logger.info("[doPartialSync]" + redisSlave);
		}
		SimpleStringParser simpleStringParser = null;
		
		if(!redisSlave.capaOf(CAPA.PSYNC2)){
			simpleStringParser = new SimpleStringParser(DefaultPsync.PARTIAL_SYNC);
		}else{
			simpleStringParser = new SimpleStringParser(String.format("%s %s", DefaultPsync.PARTIAL_SYNC, replId));
		}
		
		redisSlave.sendMessage(simpleStringParser.format());
		redisSlave.markPsyncProcessed();

		redisSlave.beginWriteCommands(new OffsetReplicationProgress(offset));
		redisSlave.partialSync();

		((RedisKeeperServer)redisSlave.getRedisServer()).getKeeperMonitor().getKeeperStats().increatePartialSync();
	}

	protected void doKeeperPartialSync(RedisSlave redisSlave, String replId, long continueOffset) {
		SimpleStringParser simpleStringParser = new SimpleStringParser(String.format("%s %s %d",
				DefaultPsync.PARTIAL_SYNC, replId, continueOffset));

		redisSlave.sendMessage(simpleStringParser.format());
		redisSlave.markPsyncProcessed();

		redisSlave.beginWriteCommands(new OffsetReplicationProgress(continueOffset));
		redisSlave.partialSync();

		((RedisKeeperServer)redisSlave.getRedisServer()).getKeeperMonitor().getKeeperStats().increatePartialSync();
	}

	@Override
	public String[] getCommands() {
		
		return new String[]{"psync"};
	}
}
