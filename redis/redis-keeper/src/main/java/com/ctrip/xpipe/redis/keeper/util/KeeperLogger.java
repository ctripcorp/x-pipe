package com.ctrip.xpipe.redis.keeper.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Nov 22, 2016
 */
public class KeeperLogger {
	
	public static final String delayTraceLog = "DelayTraceLog";
	
	
	public static Logger getDelayTraceLog(){
		
		return LoggerFactory.getLogger(delayTraceLog);
	}

}
