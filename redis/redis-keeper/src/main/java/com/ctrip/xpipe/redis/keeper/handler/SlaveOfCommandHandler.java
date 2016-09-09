package com.ctrip.xpipe.redis.keeper.handler;


import java.net.InetSocketAddress;

import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;
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
	protected void doHandle(String[] args, RedisClient redisClient) {
				
		if (validateArgs(args)) {
			
			if(args.length == 2){
//				handleSelf(args, redisClient);
				//fool the sentinel since we will use meta server to change master
				handleDummy(args, redisClient);
			}else if(args.length == 4){//forward
				handleForwarding(args, redisClient);
			}else {
				redisClient.sendMessage(new RedisErrorParser("wrong format").format());	
			}
			return;
		} 
		
		redisClient.sendMessage(new RedisErrorParser("wrong format").format());	
	}

	private void handleForwarding(String[] args, RedisClient redisClient) {
		
		if(args[0].equalsIgnoreCase(NO)){
			String ip = args[2];
			int port = Integer.parseInt(args[3]); // already validated
			
			try {
				redisClient.getRedisKeeperServer().promoteSlave(ip, port);
				redisClient.sendMessage(new BulkStringParser(RedisProtocol.OK).format());
				return;
			} catch (RedisSlavePromotionException e) {
				logger.error("[doHandle]{},{},{}", redisClient, ip, port);
				redisClient.sendMessage(new RedisErrorParser(e.getMessage()).format());	
			}
		}
	}

	private void handleDummy(String[] args, RedisClient redisClient) {
	    if (args[0].equalsIgnoreCase(NO)) {
			/**
			 * if reply OK to slaveof no one, then sentinel is found crash
			 * because sentinel thinks the keeper is the new master while it is actually not?
             */
			redisClient.sendMessage(new RedisErrorParser("Keeper not allowed to process slaveof command").format());
		} else {
			redisClient.sendMessage(SimpleStringParser.OK);
		}
	}

	private void handleSelf(String[] args, RedisClient redisClient) {
		
		if(args[0].equalsIgnoreCase(NO)){//
			logger.info("[handleSelf][promote self to master]");
			redisClient.getRedisKeeperServer().stopAndDisposeMaster();
		}else{
			logger.info("[handleSelf][slaveof]{}", StringUtil.join(" ", args));
			redisClient.getRedisKeeperServer().getRedisKeeperServerState().setMasterAddress(new InetSocketAddress(args[0], Integer.parseInt(args[1])));
		}
		redisClient.sendMessage(new BulkStringParser(RedisProtocol.OK).format());
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

}
