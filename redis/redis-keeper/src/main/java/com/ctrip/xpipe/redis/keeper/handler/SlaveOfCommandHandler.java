package com.ctrip.xpipe.redis.keeper.handler;


import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;
import com.ctrip.xpipe.utils.IpUtils;

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
		
		String errorMessage = null;
		
		if (validateArgs(args)) {
			if(args[0].equalsIgnoreCase(NO)){
				final String ip = args[2];
				final int port = Integer.parseInt(args[3]); // already validated
				
				try {
					redisClient.getRedisKeeperServer().promoteSlave(ip, port);
					redisClient.sendMessage(new BulkStringParser(RedisProtocol.OK).format());
					return;
				} catch (RedisSlavePromotionException e) {
					logger.error("[doHandle]{},{},{}", redisClient, ip, port);
					errorMessage = e.getMessage();
				}
			}else{
				errorMessage = "slave connected to keeper, discard command!";
			}
		} else {
			errorMessage = "wrong format";
		}
		
		if(errorMessage != null){
			redisClient.sendMessage(new BulkStringParser(errorMessage).format());
		}
	}

	private boolean validateArgs(String[] args) {
		try {
			if (args.length == 4) {
				
				if(args[0].equalsIgnoreCase(NO) || IpUtils.isValidIpFormat(args[0])){
					if(args[1].equalsIgnoreCase(ONE) || IpUtils.isPort(args[1])){
						if (IpUtils.isValidIpFormat(args[2]) && IpUtils.isPort(args[3])) {
							return true;
						}
					}
				}
				
			}
		} catch (Exception e) {
			// ignore
		}
		return false;
	}

}
