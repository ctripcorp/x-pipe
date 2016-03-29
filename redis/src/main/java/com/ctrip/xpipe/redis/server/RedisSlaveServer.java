package com.ctrip.xpipe.redis.server;

import com.ctrip.xpipe.redis.protocal.PsyncObserver;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午3:09:23
 */
public interface RedisSlaveServer extends RedisServer, PsyncObserver{
	
	long getReploffset();
}
