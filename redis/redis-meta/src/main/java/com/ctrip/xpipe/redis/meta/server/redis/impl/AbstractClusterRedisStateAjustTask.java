package com.ctrip.xpipe.redis.meta.server.redis.impl;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.meta.server.redis.ClusterRedisStateAjustTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Dec 26, 2016
 */
public abstract class AbstractClusterRedisStateAjustTask extends AbstractExceptionLogTask implements ClusterRedisStateAjustTask{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

}
