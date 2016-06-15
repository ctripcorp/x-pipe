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

	@Override
	public List<String> getMetaServerList() {
		// TODO
		String ip = System.getProperty("metaServerIp", "127.0.0.1");
		String port = System.getProperty("metaServerPort", "9747");
		return Arrays.asList(ip + ":" + port);
	}

}
