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
		return Arrays.asList("127.0.0.1:9747");
	}

}
