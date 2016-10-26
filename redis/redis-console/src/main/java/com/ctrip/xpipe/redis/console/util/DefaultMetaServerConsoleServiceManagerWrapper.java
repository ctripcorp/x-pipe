package com.ctrip.xpipe.redis.console.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.constant.XpipeConsoleConstant;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleServiceManager;

/**
 * @author shyin
 *
 * Sep 9, 2016
 */
public class DefaultMetaServerConsoleServiceManagerWrapper implements MetaServerConsoleServiceManagerWrapper{
	private Codec codec = Codec.DEFAULT;
	
	@Autowired
	private ConsoleConfig config;
	@Autowired
	private MetaServerConsoleServiceManager metaServerConsoleServiceManager;
	
	
	@Override
	public List<MetaServerConsoleService> get(List<String> dcNames) {
		List<MetaServerConsoleService> result = new ArrayList<MetaServerConsoleService>(dcNames.size());
		for(String addr : fetchMetaServerAddress(dcNames)) {
			result.add(metaServerConsoleServiceManager.getOrCreate(addr));
		}
		return result;
	}

	@Override
	public MetaServerConsoleService get(String dcName) {
		return metaServerConsoleServiceManager.getOrCreate(fetchMetaServerAddress(dcName));
	}
	
	private List<String> fetchMetaServerAddress(List<String> dcNames) {
		List<String> result = new ArrayList<String>(dcNames.size());
		
		Map<String, String> metaservers = fetchMetaServerConfig();
		if(null != metaservers) {
			for(String dcName : dcNames) {
				if(null != metaservers.get(dcName)) {
					result.add(metaservers.get(dcName));
				} else {
					result.add(XpipeConsoleConstant.DEFAULT_ADDRESS);
				}
			}
		}
		
		return result;
	}
	
	private String fetchMetaServerAddress(String dcName) {
		Map<String, String> metaservers = fetchMetaServerConfig();
		if(null != metaservers) {
			if(null != metaservers.get(dcName)) {
				return metaservers.get(dcName);
			}
		}
		return XpipeConsoleConstant.DEFAULT_ADDRESS;
		
	}
	
	@SuppressWarnings("unchecked")
	private Map<String,String> fetchMetaServerConfig() {
		String metaservers = config.getMetaservers();
		if(null == metaservers) {
			throw new ServerException("Cannot fetch metaservers' config");
		}
		return codec.decode(metaservers, Map.class);
	}

}
