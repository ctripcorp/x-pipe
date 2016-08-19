package com.ctrip.xpipe.redis.core.redis;

/**
 * @author wenchao.meng
 *
 * Aug 19, 2016
 */
public interface RunidGenerator {
	
	public static RunidGenerator DEFAULT = new DefaultRunIdGenerator();
	
	String generateRunid();

}