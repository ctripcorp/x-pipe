package com.ctrip.xpipe.redis.data.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.redis.data.RedisData;

/**
 * @author wenchao.meng
 *
 * 2016年4月24日 下午9:10:54
 */
public abstract class AbstractRedisData implements RedisData{
	
	protected Logger logger = LogManager.getLogger(getClass());

}
