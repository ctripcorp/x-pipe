package com.ctrip.xpipe.redis.keeper.handler;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.keeper.CommandHandler;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.utils.StringUtil;


/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:47:11
 */
public class CommandHandlerManager extends AbstractCommandHandler{

	private Map<String, CommandHandler>  handlers = new ConcurrentHashMap<String, CommandHandler>();

	public CommandHandlerManager() {
		initCommands();
	}

	private void initCommands() {
		
		putHandler(new ReplconfHandler());
		putHandler(new PsyncHandler());
		putHandler(new PingCommandHandler());
		putHandler(new LFHandler());
		putHandler(new InfoHandler());
		putHandler(new SlaveOfCommandHandler());
		putHandler(new KinfoCommandHandler());
		putHandler(new KeeperCommandHandler());
	}

	private void putHandler(CommandHandler handler) {
		
		for(String commandName : handler.getCommands()){
			
			handlers.put(commandName.toLowerCase(), handler);
		}
	}
	
	@Override
	public String[] getCommands() {		
		return handlers.keySet().toArray(new String[handlers.size()]);
	}

	@Override
	protected void doHandle(String[] args, RedisClient redisClient) {
		
		if(args.length == 0){
			logger.error("[doHandle][arg length]" + redisClient);
			return;
		}
		
		CommandHandler handler = handlers.get(args[0].toLowerCase());
		if(handler == null){
			logger.error("[doHandler][no handler found]" + StringUtil.join(" ", args));
			redisClient.sendMessage(new RedisErrorParser("unsupported command:" + args[0]).format());
			return;
		}
		
		String[] newArgs = new String[args.length - 1];
		System.arraycopy(args, 1, newArgs, 0, args.length - 1);
		if(handler.isLog(newArgs)){
			logger.info("[doHandle]{},{}", redisClient, StringUtil.join(" ", args));
		}
		handler.handle(newArgs, redisClient);
	}

}
