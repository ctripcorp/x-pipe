package com.ctrip.xpipe.redis.keeper.meta;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

/**
 * @author marsqing
 *
 *         May 30, 2016 2:25:03 PM
 */
@Component
public class DefaultMetaServerLocator implements MetaServerLocator {
	
	private String address = System.getProperty("metaServerIp", "127.0.0.1") + ":"  + System.getProperty("metaServerPort", "9747");  
		
	public DefaultMetaServerLocator() {
		
	}

	@Override
	public List<String> getMetaServerList() {
		
		return Arrays.asList(address);
	}
	
	public void setAddress(String address) {
		this.address = address;
	}

}
