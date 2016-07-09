package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public class KeeperHandler extends AbstractCommandHandler{

	@Override
	public String[] getCommands() {
		return new String[]{"keeper"};
	}

	@Override
	protected void doHandle(String[] args, RedisClient redisClient) {
		
		if(args.length >= 1){
			if(args[0].equalsIgnoreCase("getstate")){
				
			}else if(args[0].equalsIgnoreCase("setstate")){
				if(args.length >= 2){
					KeeperState keeperState = KeeperState.valueOf(args[1]);
					
				}else{
					throw new IllegalArgumentException("setstate argument error:" + StringUtil.join(" ", args));
				}
			}
		}
	}

}
