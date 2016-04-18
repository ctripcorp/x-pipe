package com.ctrip.xpipe.redis.protocal.protocal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.redis.protocal.RedisProtocol;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午6:29:33
 */
public abstract class AbstractRedisProtocol implements RedisProtocol{

	protected Logger logger = LogManager.getLogger(getClass());
	
	
}
