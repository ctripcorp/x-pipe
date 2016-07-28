package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.cluster.ShardArrange;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
@Component
public class SingleMetaServerShardArrange implements ShardArrange<String>{

	@Override
	public boolean responsableFor(String clusterId) {
		return true;
	}

}
