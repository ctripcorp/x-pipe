package com.ctrip.xpipe.redis.core.metaserver;

import com.ctrip.xpipe.redis.core.config.MetaServerAddressAware;
import com.google.common.base.Strings;

import java.util.Arrays;
import java.util.List;

/**
 * @author marsqing
 *
 *         May 30, 2016 2:25:03 PM
 */
public class DefaultMetaServerLocator implements MetaServerLocator {
	
	private MetaServerAddressAware metaServerAddressAware;
    private String addressOverride;

	public DefaultMetaServerLocator(MetaServerAddressAware metaServerAddressAware) {
		this.metaServerAddressAware = metaServerAddressAware;
		
	}

	@Override
	public List<String> getMetaServerList() {
		return Arrays.asList(getAddress());
	}
	
	public void setAddress(String address) {
		this.addressOverride = address;
	}

	private String getAddress() {
		return Strings.isNullOrEmpty(addressOverride) ? metaServerAddressAware.getMetaServerUrl() : addressOverride;
	}

}
