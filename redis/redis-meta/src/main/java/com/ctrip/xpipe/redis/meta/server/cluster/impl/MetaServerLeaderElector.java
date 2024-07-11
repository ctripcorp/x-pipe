package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.cluster.AbstractLeaderElector;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author wenchao.meng
 *
 * Jul 21, 2016
 */
@Component
public class MetaServerLeaderElector extends AbstractLeaderElector implements TopElement{

	@Autowired
	private MetaServerConfig config;

	public 	MetaServerLeaderElector() {
		setLeaderAwareClass(MetaServerLeaderAware.class);
	}

	@Override
	protected String getServerId() {
		return String.valueOf(config.getMetaServerId());
	}

	@Override
	protected String getLeaderElectPath() {
		return MetaZkConfig.getMetaServerLeaderElectPath();
	}
}
