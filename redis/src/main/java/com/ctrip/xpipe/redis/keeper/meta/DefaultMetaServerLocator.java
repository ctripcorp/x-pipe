package com.ctrip.xpipe.redis.keeper.meta;

import java.util.LinkedList;
import java.util.List;

import org.springframework.stereotype.Component;

/**
 * @author marsqing
 *
 *         May 30, 2016 2:25:03 PM
 */
@Component
public class DefaultMetaServerLocator implements MetaServerLocator {
	
	private List<String> metaServerList = new LinkedList<>();
	
	public DefaultMetaServerLocator() {
		
		metaServerList.add("127.0.0.1:9747");
	}

	@Override
	public List<String> getMetaServerList() {
		
		return metaServerList;
	}
	
	public void clear(){
		metaServerList.clear();
	}

	public void add(String metaServer){
		metaServerList.add(metaServer);
	}
}
