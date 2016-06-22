package com.ctrip.xpipe.redis.core.protocal.protocal;

import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;

import org.slf4j.Logger;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午6:29:33
 */
public abstract class AbstractRedisProtocol implements RedisProtocol{

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	
}
