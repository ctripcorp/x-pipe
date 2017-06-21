package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import com.ctrip.xpipe.cluster.AbstractLeaderElector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;

/**
 * @author wenchao.meng
 *
 * Jul 21, 2016
 */
@Component
public class MetaserverLeaderElector extends AbstractLeaderElector implements TopElement{

	@Autowired
	private MetaServerConfig config;

	@Override
	protected String getServerId() {
		return String.valueOf(config.getMetaServerId());
	}

	@Override
	protected String getLeaderElectPath() {
		return MetaZkConfig.getMetaServerLeaderElectPath();
	}
}
