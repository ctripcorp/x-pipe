package com.ctrip.xpipe.redis.keeper.handler;


import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.keeper.CommandHandler;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.handler.keeper.*;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:47:11
 */
public class CommandHandlerManager extends AbstractCommandHandler {

	private Map<String, CommandHandler> handlers = new ConcurrentHashMap<>();

	public CommandHandlerManager() {
		initCommands();
	}

	protected void initCommands() {
		
		putHandler(new ReplconfHandler());
		// putHandler(new PsyncHandler());
		putHandler(new PingCommandHandler());
		putHandler(new LFHandler());
		putHandler(new InfoHandler());
		putHandler(new SlaveOfCommandHandler());
		putHandler(new KinfoCommandHandler());
		putHandler(new KeeperCommandHandler());
		putHandler(new PublishCommandHandler());
		putHandler(new SubscribeCommandHandler());
		putHandler(new ClientCommandHandler());
		putHandler(new RoleCommandHandler());
		putHandler(new ProxyCommandHandler());
		putHandler(new ConfigHandler());
		putHandler(new GapAllowPSyncHandler());
		putHandler(new GapAllowXSyncHandler());
	}

	protected void putHandler(CommandHandler handler) {
		
		for(String commandName : handler.getCommands()){
			
			handlers.put(commandName.toLowerCase(), handler);
		}
	}

	@Override
	public String[] getCommands() {		
		return handlers.keySet().toArray(new String[handlers.size()]);
	}

	@Override
	protected void doHandle(final String[] args, final RedisClient<?> redisClient) {
		if (args.length == 0) {
			logger.error("[doHandle][arg length]{}", redisClient);
			return;
		}


		redisClient.getRedisServer().processCommandSequentially(new Runnable() {

			@Override
			public void run() {
				try {
					CommandHandler handler = handlers.get(args[0].toLowerCase());
					if (handler == null) {
						logger.error("[doHandler][no handler found]{}, {}", redisClient, StringUtil.join(" ", args));
						redisClient.sendMessage(new RedisErrorParser("unsupported command:" + args[0]).format());
						return;
					}
					if (!handler.support(redisClient.getRedisServer())) {
						logger.error("[doHandler][server not support]{}, {}", redisClient, StringUtil.join(" ", args));
						redisClient.sendMessage(new RedisErrorParser("unsupported command:" + args[0]).format());
						return;
					}
					innerDoHandle(args, redisClient, handler);
				} catch (Throwable th) {
					logger.error("Error process command {} for client {}", Arrays.asList(args), redisClient, th);
					redisClient.sendMessage(new RedisErrorParser(th.getMessage()).format());
				}
			}

			@Override
			public String toString() {
				return String.format("%s, %s", redisClient, StringUtil.join(" ", args));
			}
		});
	}
	
	private void innerDoHandle(String[] args, RedisClient<?> redisClient, CommandHandler handler) throws Exception {
		String[] newArgs = new String[args.length - 1];
		System.arraycopy(args, 1, newArgs, 0, args.length - 1);
		if(handler.isLog(newArgs)){
			logger.info("[doHandle]{},{}", redisClient, StringUtil.join(" ", args));
		}else{
			logger.debug("[doHandle]{},{}", redisClient, StringUtil.join(" ", args));
		}
		handler.handle(newArgs, redisClient);
	}

}
