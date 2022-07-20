package com.ctrip.xpipe.redis.console.util;

import com.ctrip.xpipe.redis.checker.MetaServerManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleServiceManager;
import com.ctrip.xpipe.redis.core.metaserver.MetaserverAddress;
import com.ctrip.xpipe.redis.core.metaserver.impl.DefaultMetaServerConsoleServiceManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author shyin
 *
 * Sep 9, 2016
 */
public class DefaultMetaServerConsoleServiceManagerWrapper implements MetaServerConsoleServiceManagerWrapper, MetaServerManager {

	private ConsoleConfig config = new DefaultConsoleConfig();

	private MetaServerConsoleServiceManager metaServerConsoleServiceManager = new DefaultMetaServerConsoleServiceManager();
	
	
	@Override
	public List<MetaServerConsoleService> get(List<String> dcNames) {
		List<MetaServerConsoleService> result = new ArrayList<MetaServerConsoleService>(dcNames.size());
		for(MetaserverAddress addr : fetchMetaServerAddress(dcNames)) {
			result.add(metaServerConsoleServiceManager.getOrCreate(addr));
		}
		return result;
	}

	@Override
	public MetaServerConsoleService get(String dcName) {
		return metaServerConsoleServiceManager.getOrCreate(fetchMetaServerAddress(dcName));
	}

	@Override
	public MetaServerConsoleService getFastService(String dcName) {
		return metaServerConsoleServiceManager.getOrCreateFastService(fetchMetaServerAddress(dcName));
	}

	@Override
	public RedisMeta getCurrentMaster(String dcId, String clusterId, String shardId) {
		MetaServerConsoleService metaServerConsoleService = getFastService(dcId);
		if (null == metaServerConsoleService) throw new IllegalArgumentException("unknown dc " + dcId);

		return metaServerConsoleService.getCurrentMaster(clusterId, shardId);
	}

	private List<MetaserverAddress> fetchMetaServerAddress(List<String> dcNames) {
		List<MetaserverAddress> result = new ArrayList<MetaserverAddress>(dcNames.size());
		
		Map<String, String> metaservers = config.getMetaservers();
		if(null != metaservers) {
			for(String dcName : dcNames) {
				if(null != metaservers.get(dcName)) {
					result.add(new MetaserverAddress(dcName, metaservers.get(dcName)));
				} else {
					result.add(new MetaserverAddress(dcName, XPipeConsoleConstant.DEFAULT_ADDRESS));
				}
			}
		}
		
		return result;
	}
	
	private MetaserverAddress fetchMetaServerAddress(String dcName) {
		Map<String, String> metaservers = config.getMetaservers();
		if(null != metaservers) {
			if(null != metaservers.get(dcName)) {
				return new MetaserverAddress(dcName, metaservers.get(dcName));
			}
		}
		return new MetaserverAddress(dcName, XPipeConsoleConstant.DEFAULT_ADDRESS);
		
	}

}
