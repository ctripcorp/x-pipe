package com.ctrip.xpipe.redis.core.protocal;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午6:27:48
 */
public interface RedisProtocol {
	
	int REDIS_PORT_DEFAULT = 6379;
	int RUN_ID_LENGTH = 40;
	
	String CRLF = "\r\n";
	
	String OK = "OK";

	static String booleanToString(boolean yes){

		if(yes){
			return "yes";
		}
		return "no";
	}



}
