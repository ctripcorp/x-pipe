package com.ctrip.xpipe.redis.keeper.handler.keeper;


import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.CommandBulkStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author marsqing
 *
 *         May 9, 2016 12:10:38 PM
 */
public class SlaveOfCommandHandler extends AbstractCommandHandler {

	private final static String[] COMMANDS = new String[] { "slaveof" };
	private final static String NO = "no"; 
	private final static String ONE = "one"; 

	/**
	 * slave of ip port
	 */
	@Override
	public String[] getCommands() {
		return COMMANDS;
	}

	@Override
	protected void doHandle(String[] args, RedisClient<?> redisClient) {
				
		if (validateArgs(args)) {
			
			if(args.length == 2){
//				handleSelf(args, redisClient);
				//fool the sentinel since we will use meta server to change master
				handleSlaveOf(args, redisClient);
			}else if(args.length == 4){//forward
				handleForwarding(args, redisClient);
			}else {
				redisClient.sendMessage(new RedisErrorParser("wrong format").format());	
			}
			return;
		} 
		
		redisClient.sendMessage(new RedisErrorParser("wrong format").format());	
	}

	private void handleForwarding(String[] args, RedisClient<?> redisClient) {
		
		if(args[0].equalsIgnoreCase(NO)){
			String ip = args[2];
			int port = Integer.parseInt(args[3]); // already validated
			
			try {
				((RedisKeeperServer)redisClient.getRedisServer()).promoteSlave(ip, port);
				redisClient.sendMessage(new CommandBulkStringParser(RedisProtocol.OK).format());
				return;
			} catch (RedisSlavePromotionException e) {
				logger.error("[doHandle]{},{},{}", redisClient, ip, port);
				redisClient.sendMessage(new RedisErrorParser(e.getMessage()).format());	
			}
		}
	}

	protected void handleSlaveOf(String[] args, RedisClient<?> redisClient) {
	    if (args[0].equalsIgnoreCase(NO)) {
			/**
			 * if reply OK to slaveof no one, then sentinel is found crash
			 * because sentinel thinks the keeper is the new master while it is actually not?
             */
			redisClient.sendMessage(new RedisErrorParser("Keeper not allowed to process slaveof command").format());
		} else {

			RedisKeeperServer redisKeeperServer = (RedisKeeperServer) redisClient.getRedisServer();
			String host = args[0];
			int port = Integer.parseInt(args[1]);
	    	if(redisKeeperServer.getRedisKeeperServerState().handleSlaveOf()){
				logger.info("[handleSlaveOf][slaveof]{}:{} {}", host, port, redisClient);
				((RedisKeeperServer)redisClient.getRedisServer()).getRedisKeeperServerState().setMasterAddress(
						new DefaultEndPoint(host, port));
			}else{
				logger.info("[handleSlaveOf][slaveof, ignore]{},{}:{} {}", redisKeeperServer.getRedisKeeperServerState(), host, port, redisClient);
			}
			redisClient.sendMessage(SimpleStringParser.OK);
		}
	}

	@SuppressWarnings("unused")
	private void handleSelf(String[] args, RedisClient redisClient) {
		
		if(args[0].equalsIgnoreCase(NO)){//
			logger.info("[handleSelf][promote self to master]");
			((RedisKeeperServer)redisClient.getRedisServer()).stopAndDisposeMaster();
		}else{
			logger.info("[handleSelf][slaveof]{}", StringUtil.join(" ", args));
			((RedisKeeperServer)redisClient.getRedisServer()).getRedisKeeperServerState().setMasterAddress(new DefaultEndPoint(args[0], Integer.parseInt(args[1])));
		}
		redisClient.sendMessage(new CommandBulkStringParser(RedisProtocol.OK).format());
	}

	private boolean validateArgs(String[] args) {
		try {
				
			if(args[0].equalsIgnoreCase(NO) || IpUtils.isValidIpFormat(args[0])){
				if(args[1].equalsIgnoreCase(ONE) || IpUtils.isPort(args[1])){
					return true;
				}
			}
		} catch (Exception e) {
			// ignore
		}
		return false;
	}

	@Override
	public boolean support(RedisServer server) {
		return server instanceof RedisKeeperServer;
	}

}
