package com.ctrip.xpipe.redis.keeper.data.impl;

import com.ctrip.xpipe.redis.keeper.data.RedisData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author wenchao.meng
 *
 * 2016年4月24日 下午9:10:54
 */
public abstract class AbstractRedisData implements RedisData{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

}
