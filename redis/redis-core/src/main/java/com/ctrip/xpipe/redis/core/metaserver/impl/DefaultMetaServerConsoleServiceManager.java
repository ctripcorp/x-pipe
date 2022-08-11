package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleServiceManager;
import com.ctrip.xpipe.redis.core.metaserver.MetaserverAddress;
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

	private Map<String, MetaServerConsoleService> fastServices = new ConcurrentHashMap<>();

	@Override
	public MetaServerConsoleService getOrCreate(final MetaserverAddress metaServerAddress) {

		return MapUtils.getOrCreate(services, metaServerAddress.getAddress(), new ObjectFactory<MetaServerConsoleService>() {

			@Override
			public MetaServerConsoleService create() {

				return new DefaultMetaServerConsoleService(metaServerAddress);
			}
		});
	}

	@Override
	public MetaServerConsoleService getOrCreateFastService(final MetaserverAddress metaServerAddress) {

		return MapUtils.getOrCreate(fastServices, metaServerAddress.getAddress(), new ObjectFactory<MetaServerConsoleService>() {

			@Override
			public MetaServerConsoleService create() {

				return new FastMetaServerConsoleService(metaServerAddress);
			}
		});
	}

}
