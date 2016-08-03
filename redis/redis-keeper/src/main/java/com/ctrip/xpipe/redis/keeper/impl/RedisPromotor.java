package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.cmd.Fsync;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.redis.core.protocal.error.RedisError;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.PROMOTION_STATE;

/**
 * @author marsqing
 *
 * Jun 22, 2016
 */
public class RedisPromotor {
	
	protected Logger logger = LoggerFactory.getLogger(RedisPromotor.class);
	
	private int waitTimeoutMilli = 60 * 1000;
	
	private final RedisKeeperServer redisKeeperServer;
	private final String promoteServerIp;
	private final int promoteServerPort;
	
	public RedisPromotor(RedisKeeperServer redisKeeperServer, String promoteServerIp, int promoteServerPort){
		
		this.redisKeeperServer = redisKeeperServer;
		this.promoteServerIp = promoteServerIp;
		this.promoteServerPort = promoteServerPort;
	}
	
	public void promote() throws RedisSlavePromotionException{
		

		final RedisSlave redisSlave = findSlave(this.redisKeeperServer, this.promoteServerIp, this.promoteServerPort);
		if(redisSlave == null){
			String msg = String.format("%s:%s is not a connected slave", promoteServerIp, promoteServerPort);
			throw new RedisSlavePromotionException(msg);
		}
		
		logger.info("[promote]{},{} ,{}:{}", redisKeeperServer, redisSlave, promoteServerIp, promoteServerPort);
		new Thread() {
			public void run() {
				try {
					promoteSlaveToMaster(redisSlave);
				} catch (Exception e) {
					logger.error("[run][promote slave]" + redisSlave, e);
				}
			}
		}.start();
	}

	private void promoteSlaveToMaster(RedisSlave redisSlave) throws Exception {

		SimpleObjectPool<NettyClient> fsyncPool = null;
		SimpleObjectPool<NettyClient> clientPool = null;
		try{
			fsyncPool = NettyPoolUtil.createNettyPool(new InetSocketAddress(promoteServerIp, promoteServerPort));
			clientPool = NettyPoolUtil.createNettyPool(new InetSocketAddress(promoteServerIp, promoteServerPort));
			waitUntilSlaveSync(redisSlave, this.promoteServerIp, this.promoteServerPort, waitTimeoutMilli);
			
			try{
				redisKeeperServer.getRedisKeeperServerState().setPromotionState(PROMOTION_STATE.BEGIN_PROMOTE_SLAVE);
				Fsync fsyncCmd = new Fsync(fsyncPool);
				String fsyncResult = fsyncCmd.execute().get();
				logger.info("[promoteSlaveToMaster][fsync done]{}, {},{}", fsyncResult, promoteServerIp, promoteServerPort);
				redisModified(redisSlave, clientPool);
			}catch(ExecutionException e){
				logger.error("[promoteSlaveToMaster]" + redisSlave, e.getCause());
				if(e.getCause() instanceof RedisError){
					logger.info("[promoteSlaveToMaster][fsync not supported, raw redis]{}", redisSlave);
					redisNotModified(redisSlave, clientPool);
				}else{
					logger.error("[promoteSlaveToMaster][fail]" + redisSlave);
				}
			}
		}finally{
			if(fsyncPool != null){
				fsyncPool.clear();
			}
			if(clientPool != null){
				clientPool.clear();
			}
		}
	}

	private void redisModified(RedisSlave redisSlave, SimpleObjectPool<NettyClient> clientPool) throws Exception {
		
		try{
			SlaveOfCommand slaveOfCmd = new SlaveOfCommand(clientPool);
			slaveOfCmd.execute().sync();
	
			InfoCommand infoServerCmd = new InfoCommand(clientPool, "server");
			String info = infoServerCmd.execute().get();
			String masterId = null;
	
			try{
				List<String> lines = IOUtils.readLines(new StringReader(info));
				for (String line : lines) {
					if (line.startsWith("run_id:")) {
						masterId = line.substring("run_id:".length());
					}
				}
				InfoCommand infoLastMasterCmd = new InfoCommand(clientPool,"lastmaster");
				String infoLastMaster = infoLastMasterCmd.execute().get();
				long keeperOffset = 0, newMasterOffset = 0;
				try {
					String[] parts = infoLastMaster.split("\\s");
					keeperOffset = Long.parseLong(parts[1]);
					newMasterOffset = Long.parseLong(parts[2]);
					
					redisKeeperServer.getRedisKeeperServerState().setPromotionState(
							PROMOTION_STATE.SLAVE_PROMTED, new SlavePromotionInfo(keeperOffset, new DefaultEndPoint(promoteServerIp, promoteServerPort), 
									masterId, newMasterOffset));
				} catch (Exception e) {
					logger.error("[onComplete]" + promoteServerIp + ":" + promoteServerPort, e);
				}
			} catch (IOException e1) {
				logger.error("promoteSlaveToMaster", e1);
			}
		}finally{
		}
	}

	private void redisNotModified(RedisSlave redisSlave, SimpleObjectPool<NettyClient> clientPool) throws InterruptedException, ExecutionException, IOException {
		
		SlaveOfCommand slaveOfCmd = new SlaveOfCommand(clientPool);
		slaveOfCmd.execute().sync();
		
		redisKeeperServer.getRedisKeeperServerState().setPromotionState(PROMOTION_STATE.SLAVE_PROMTED, new InetSocketAddress(promoteServerIp, promoteServerPort));
	}

	private void waitUntilSlaveSync(RedisSlave redisSlave, String ip, int port, int timeoutMilli) {
		
		long til = System.currentTimeMillis() + timeoutMilli;
		while (true) {
			long current = System.currentTimeMillis();
			if(current > til){
				logger.info("[waitUntilSlaveSync][timeout]{}", redisSlave);
				break;
			}
			
			Long slaveCmdOffset = redisSlave.getAck();
			long masterCmdOffset = redisSlave.getRedisKeeperServer().getKeeperRepl().getEndOffset();
			
			if(slaveCmdOffset == null || slaveCmdOffset < masterCmdOffset){
				if (logger.isInfoEnabled()) {
					logger.info("[waitUntilSlaveSync]{}, {} < {}", redisSlave, slaveCmdOffset, masterCmdOffset);
				}
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
			}else{
				break;
			}
		}
	}

	private RedisSlave findSlave(RedisKeeperServer keeper, String actualIp, int actualPort) {
		Set<RedisSlave> slaves = keeper.slaves();
		for (RedisSlave redisSlave : slaves) {
			
			InetSocketAddress slaveAddr = (InetSocketAddress) redisSlave.channel().remoteAddress();
			String expectedIp = slaveAddr.getAddress().getHostAddress();
			int expectedPort = redisSlave.getSlaveListeningPort();

			if (expectedIp.equals(actualIp) && expectedPort == actualPort) {
				return redisSlave;
			}
		}
		return null;
	}

	
	public static class SlavePromotionInfo{
		
		private long keeperOffset;
		private DefaultEndPoint newMasterEndpoint;
		private String newMasterRunid;
		private long newMasterReplOffset;
		
		public SlavePromotionInfo(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid, long newMasterReplOffset){
			this.keeperOffset = keeperOffset;
			this.newMasterEndpoint = newMasterEndpoint;
			this.newMasterRunid = newMasterRunid;
			this.newMasterReplOffset = newMasterReplOffset;
		}
		

		public long getKeeperOffset() {
			return keeperOffset;
		}

		public DefaultEndPoint getNewMasterEndpoint() {
			return newMasterEndpoint;
		}

		public String getNewMasterRunid() {
			return newMasterRunid;
		}

		public long getNewMasterReplOffset() {
			return newMasterReplOffset;
		}
		
		@Override
		public String toString() {
			return String.format(
					"keeperOffset:%d, newMasterEndpoint:%s, newMasterRunid:%s, newMasterReplOffset:%d",
					keeperOffset, newMasterEndpoint.toString(), newMasterRunid, newMasterReplOffset
					);
		}
	}
}
