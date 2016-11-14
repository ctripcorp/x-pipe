package com.ctrip.xpipe.redis.integratedtest.stability;

import org.apache.commons.lang3.tuple.Pair;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

/**
 * @author wenchao.meng
 *
 * Nov 14, 2016
 */
public interface ValueCheck extends Startable, Stoppable{

	void offer(Pair<String, String> checkData);
	
	long queueSize();

}
