package com.ctrip.xpipe.redis.core.protocal;

import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午6:27:48
 */
public interface RedisProtocol {
	
	int REDIS_PORT_DEFAULT = 6379;
	int KEEPER_PORT_DEFAULT = 6380;
	int APPLIER_PORT_DEFAULT = 16000;
	int RUN_ID_LENGTH = 40;
	
	String CRLF = "\r\n";
	
	String OK = "OK";

	String KEEPER_ROLE_PREFIX = "keeperrole";

	static String booleanToString(boolean yes){

		if(yes){
			return "yes";
		}
		return "no";
	}

	static Boolean stringToBoolean(String val){
		if(StringUtil.trimEquals("yes", val)) {
			return true;
		} else {
			return false;
		}
	}



}
