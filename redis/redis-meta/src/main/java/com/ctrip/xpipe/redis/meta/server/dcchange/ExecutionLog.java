package com.ctrip.xpipe.redis.meta.server.dcchange;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public class ExecutionLog {
	
	private static Logger logger = LoggerFactory.getLogger(ExecutionLog.class);
	
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    
	private StringBuilder log = new StringBuilder();
	
	public void info(String message){
		log.append(format("info", message));
		logger.info("{}", message);
	}

	public void warn(String message){
		log.append(format("warn", message));
		logger.warn("{}", message);
	}

	public void error(String message){
		log.append(format("error", message));
		logger.error("{}", message);
	}

	
	private String format(String tag, String message) {
		
		return String.format("[%s][%s]%s\n", tag, currentTime(), message);
	}


	private String currentTime() {
		return sdf.format(new Date());
	}
	
	public String getLog() {
		return log.toString();
	}
}
