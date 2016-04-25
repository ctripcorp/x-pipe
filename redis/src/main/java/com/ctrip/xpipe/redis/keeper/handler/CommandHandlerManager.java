package com.ctrip.xpipe.redis.keeper.handler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.ctrip.xpipe.redis.keeper.CommandHandler;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.utils.StringUtil;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:47:11
 */
public class CommandHandlerManager extends AbstractCommandHandler{

	private Map<String, CommandHandler>  handlers = new ConcurrentHashMap<String, CommandHandler>();
	
	public static enum COMMAND_STATE{
		READ_SIGN,
		READ_COMMANDS
	}
	
	private COMMAND_STATE commandState = COMMAND_STATE.READ_SIGN;
	private RedisClientProtocol<?>  redisClientProtocol;

	public CommandHandlerManager() {
		initCommands();
	}

	private void initCommands() {
		
		putHandler(new ReplconfHandler());
		putHandler(new PsyncHandler());
		putHandler(new PingCommandHandler());
	}

	private void putHandler(CommandHandler handler) {
		
		for(String commandName : handler.getCommands()){
			
			handlers.put(commandName.toLowerCase(), handler);
		}
	}
	
	public void handle(ByteBuf byteBuf, RedisClient redisClient){
		
		
		while(true){

			switch(commandState){
				case READ_SIGN:
					if(!hasDataRead(byteBuf)){
						return;
					}
					int readIndex = byteBuf.readerIndex();
					if(byteBuf.getByte(readIndex) == RedisClientProtocol.ASTERISK_BYTE){
						redisClientProtocol = new ArrayParser();
					}else{
						redisClientProtocol = new SimpleStringParser();
					}
					commandState = COMMAND_STATE.READ_COMMANDS;
				case READ_COMMANDS:
					RedisClientProtocol<?> resultParser = redisClientProtocol.read(byteBuf);
					if(resultParser == null){
						return;
					}
					
					Object result = resultParser.getPayload();
					if(result == null){
						return;
					}
					
					commandState = COMMAND_STATE.READ_SIGN ;
					if(result instanceof String){
						handleString((String)result, redisClient);
					}else if(result instanceof Object[]){
						handleArray((Object[])result, redisClient);
					}
					break;
				default:
					throw new IllegalStateException("unkonwn state:" + commandState);
			}
		}
	}

	private void handleArray(Object[] result, RedisClient redisClient) {

		String []strArray = new String[result.length];
		int index = 0;
		for(Object param : result){
			strArray[index++] = (String) param;
		}
		handle(strArray, redisClient);
	}

	private void handleString(String result, RedisClient redisClient) {
		
		handle(result.trim().split("\\s+"), redisClient);
	}

	private boolean hasDataRead(ByteBuf byteBuf) {
		
		if(byteBuf.readableBytes() > 0){
			return true;
		}
		return false;
	}

	@Override
	public String[] getCommands() {		
		return handlers.keySet().toArray(new String[handlers.size()]);
	}

	@Override
	protected void doHandle(String[] args, RedisClient redisClient) {
		
		CommandHandler handler = handlers.get(args[0].toLowerCase());
		if(handler == null){
			logger.error("[doHandler][no handler found]" + StringUtil.join(" ", args));
			redisClient.sendMessage(new RedisErrorParser("unsupported command" + args[0]).format());
			return;
		}
		
		String[] newArgs = new String[args.length - 1];
		System.arraycopy(args, 1, newArgs, 0, args.length - 1);
		handler.handle(newArgs, redisClient);
	}

}
