package com.ctrip.xpipe.redis.meta.server.dcchange;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.utils.LogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public class ExecutionLog {
	
	private static Logger logger = LoggerFactory.getLogger(ExecutionLog.class);
	
	private StringBuffer log = new StringBuffer();

	public ExecutionLog(String initMessage){
		info(initMessage);
	}
	
	public void info(String message){
		log.append(LogUtils.info(message));
		logger.info("{}", message);
	}

	public void warn(String message){
		log.append(LogUtils.warn(message));
		logger.warn("{}", message);
	}

	public void error(String message){
		log.append(LogUtils.error(message));
		logger.error("{}", message);
	}

	public String getLog() {
		return log.toString();
	}

	public Command trackCommand(Object invoker, AbstractCommand command, String desc) {
		command.future().addListener(commandFuture -> {
			if (commandFuture.isSuccess()) {
				info(String.format("%s : %s", desc, commandFuture.get()));
			} else {
				warn(String.format("%s : %s", desc, commandFuture.cause().getMessage()));
				LoggerFactory.getLogger(invoker.getClass()).warn("[{}]{}", command.getName(), desc, commandFuture.cause());
			}
		});
		return command;
	}

}
