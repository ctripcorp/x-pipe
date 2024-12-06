package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.redis.core.protocal.CAPA;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:49:14
 */
public class ReplconfHandler extends AbstractCommandHandler {

	@Override
	public String[] getCommands() {
		return new String[]{"replconf"};
	}

	@Override
	protected void doHandle(String[] args, RedisClient<?> redisClient) {
		
		if(args.length == 0){
			throw new IllegalArgumentException("argument error length 0");
		}
		String option = args[0];
		
		if("listening-port".equalsIgnoreCase(option)){
			redisClient.setSlaveListeningPort(Integer.valueOf(args[1]));
		}else if("capa".equalsIgnoreCase(option)){
			for(int i=0;i<args.length;i++){
				i++;
				redisClient.capa(CAPA.of(args[i]));
			}
		}else if("ack".equalsIgnoreCase(option)){
			
			if(redisClient instanceof RedisSlave){
				((RedisSlave)redisClient).ack(Long.valueOf(args[1]), true);
			}else{
				logger.warn("[replconf ack received, but client is not slave]" + redisClient + "," + StringUtil.join(" ", args));
			}
			return;
		}else if("getack".equalsIgnoreCase(option)){
			throw new IllegalStateException("[doHandle][getack not supported]" );
		}else if("keeper".equalsIgnoreCase(option)){//extends by keeper
			redisClient.setKeeper();
		}else if(Replconf.ReplConfType.IDC.name().equalsIgnoreCase(option)) {
			redisClient.setIdc(args[1].toUpperCase());
		} else{
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

	@Override
	public boolean support(RedisServer server) {
		return server instanceof RedisKeeperServer;
	}

}
