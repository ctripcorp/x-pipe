package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleServiceManager;
import com.ctrip.xpipe.utils.MapUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wenchao.meng
 *
 *         Sep 5, 2016
 */
public class DefaultMetaServerConsoleServiceManager implements MetaServerConsoleServiceManager {

	private Map<String, MetaServerConsoleService> services = new ConcurrentHashMap<>();

	@Override
	public MetaServerConsoleService getOrCreate(final String metaServerAddress) {

		return MapUtils.getOrCreate(services, metaServerAddress, new ObjectFactory<MetaServerConsoleService>() {

			@Override
			public MetaServerConsoleService create() {

				return new DefaultMetaServerConsoleService(metaServerAddress);
			}
		});
	}

}
