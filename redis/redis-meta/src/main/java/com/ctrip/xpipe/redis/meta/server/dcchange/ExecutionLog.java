package com.ctrip.xpipe.redis.meta.server.dcchange;

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
	
	private StringBuilder log = new StringBuilder();

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
}
