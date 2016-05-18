package com.ctrip.xpipe.redis.keeper.handler;


import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisClient.CAPA;
import com.ctrip.xpipe.redis.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:49:14
 */
public class ReplconfHandler extends AbstractCommandHandler{

	@Override
	public String[] getCommands() {
		return new String[]{"replconf"};
	}

	@Override
	protected void doHandle(String[] args, RedisClient redisClient) {
		
		if(args.length == 0){
			throw new IllegalArgumentException("argument error length 0");
		}
		
		if("listening-port".equalsIgnoreCase(args[0])){
			redisClient.setSlaveListeningPort(Integer.valueOf(args[1]));
		}else if("capa".equalsIgnoreCase(args[0])){
			redisClient.capa(CAPA.of(args[1]));
		}else if("ack".equalsIgnoreCase(args[0])){
			redisClient.ack(Long.valueOf(args[1]));
			return;
		}else if("getack".equalsIgnoreCase(args[0])){
			throw new IllegalStateException("[doHandle][getack not supported]" );
		}else{
			logger.error("[doHandler][unkonwn command]" + StringUtil.join(" ", args));
			redisClient.sendMessage(new RedisErrorParser("unknown replconf command " + StringUtil.join(" ", args)).format());
			return;
		}
		
		redisClient.sendMessage(SimpleStringParser.OK);
	}
	

	@Override
	public boolean isLog(String[] args) {
		
		if(args.length >= 1 && args[0].equalsIgnoreCase("ack")){
			return false;
		}
		return true;
	}
}
