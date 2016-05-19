package com.ctrip.xpipe.redis.protocal.protocal;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.ctrip.xpipe.redis.protocal.RedisProtocol;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午6:29:33
 */
public abstract class AbstractRedisProtocol implements RedisProtocol{

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	
}
