package com.ctrip.xpipe.redis.keeper.data.impl;

import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.redis.keeper.data.RedisData;

import org.slf4j.Logger;


/**
 * @author wenchao.meng
 *
 * 2016年4月24日 下午9:10:54
 */
public abstract class AbstractRedisData implements RedisData{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

}
