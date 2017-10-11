package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcServiceManager;
import com.ctrip.xpipe.utils.MapUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wenchao.meng
 *
 *         Sep 5, 2016
 */
public class DefaultMetaServerMultiDcServiceManager implements MetaServerMultiDcServiceManager {

	private Map<String, MetaServerMultiDcService> services = new ConcurrentHashMap<>();

	@Override
	public MetaServerMultiDcService getOrCreate(final String metaServerAddress) {

		return MapUtils.getOrCreate(services, metaServerAddress, new ObjectFactory<MetaServerMultiDcService>() {

			@Override
			public MetaServerMultiDcService create() {

				return new DefaultMetaServerMultiDcService(metaServerAddress);
			}
		});
	}

}
