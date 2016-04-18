package com.ctrip.xpipe.redis.data.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.redis.data.RedisData;

/**
 * @author wenchao.meng
 *
 * 2016年3月30日 下午5:15:31
 */
public abstract class AbstractRedisData implements RedisData{
	
	protected Logger logger = LogManager.getLogger(getClass());

}
