package com.ctrip.xpipe.redis.keeper.handler;


import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractKeeperCommand;
import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.utils.StringUtil;

import java.net.InetSocketAddress;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public class KeeperCommandHandler extends AbstractCommandHandler{

	@Override
	public String[] getCommands() {
		return new String[]{"keeper"};
	}

	@Override
	protected void doHandle(String[] args, RedisClient redisClient) {
		
		if(args.length >= 1){
			
			if(args[0].equalsIgnoreCase(AbstractKeeperCommand.GET_STATE)){
				
				KeeperState keeperState = redisClient.getRedisKeeperServer().getRedisKeeperServerState().keeperState();
				redisClient.sendMessage(new SimpleStringParser(keeperState.toString()).format());
			}else if(args[0].equalsIgnoreCase(AbstractKeeperCommand.SET_STATE)){
				
				if(args.length >= 4){
					KeeperState keeperState = KeeperState.valueOf(args[1]);
					InetSocketAddress masterAddress = new InetSocketAddress(args[2], Integer.parseInt(args[3]));
					doSetKeeperState(redisClient, keeperState, masterAddress);
				}else{
					throw new IllegalArgumentException("setstate argument error:" + StringUtil.join(" ", args));
				}
			}else{
				throw new IllegalStateException("unknown command:" + args[0]);
			}
		}
	}

	private void doSetKeeperState(RedisClient redisClient, KeeperState keeperState, InetSocketAddress masterAddress) {
		
		RedisKeeperServer redisKeeperServer = redisClient.getRedisKeeperServer();
		
		RedisKeeperServerState currentState = redisKeeperServer.getRedisKeeperServerState();
		try{
			switch(keeperState){
				case ACTIVE:
					currentState.becomeActive(masterAddress);
					break;
				case BACKUP:
					currentState.becomeBackup(masterAddress);
					break;
				case UNKNOWN:
					throw new IllegalStateException("state can not change to unknown!");
				default:
					throw new IllegalStateException("unrecognised state:" + keeperState);
			}
			redisClient.sendMessage(new SimpleStringParser(RedisProtocol.OK).format());
		}catch(Exception e){
			logger.error("[doSetKeeperState]" + String.format("%s, %s, %s", redisClient, keeperState, masterAddress), e);
			redisClient.sendMessage(new RedisErrorParser(e.getMessage()).format());
		}
	}
}
