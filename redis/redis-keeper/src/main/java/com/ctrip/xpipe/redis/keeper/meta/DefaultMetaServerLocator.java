package com.ctrip.xpipe.redis.keeper.meta;

import java.util.Arrays;
import java.util.List;

import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
import com.google.common.base.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author marsqing
 *
 *         May 30, 2016 2:25:03 PM
 */
@Component
public class DefaultMetaServerLocator implements MetaServerLocator {
	@Autowired
	private KeeperContainerConfig keeperContainerConfig;
    private String addressOverride;

	public DefaultMetaServerLocator() {
		
	}

	@Override
	public List<String> getMetaServerList() {
		return Arrays.asList(getAddress());
	}
	
	public void setAddress(String address) {
		this.addressOverride = address;
	}

	private String getAddress() {
		return Strings.isNullOrEmpty(addressOverride) ? keeperContainerConfig.getMetaServerUrl() : addressOverride;
	}

}
