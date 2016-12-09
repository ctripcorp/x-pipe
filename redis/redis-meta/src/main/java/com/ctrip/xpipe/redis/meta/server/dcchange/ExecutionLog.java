package com.ctrip.xpipe.redis.meta.server.dcchange;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public class ExecutionLog {
	
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    
	private StringBuilder log = new StringBuilder();
	
	public void info(String message){
		log.append(format("info", message));
	}
	
	private String format(String tag, String message) {
		
		return String.format("[%s][%s]{}\n", tag, currentTime(), message);
	}

	public void error(String message){
		log.append(format("error", message));
	}
	
	private String currentTime() {
		return sdf.format(new Date());
	}
	
	public String getLog() {
		return log.toString();
	}
}
