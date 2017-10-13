package com.ctrip.xpipe.redis.console.util;

import java.util.List;

import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;

/**
 * @author shyin
 *
 * Sep 9, 2016
 */
public interface MetaServerConsoleServiceManagerWrapper {

	List<MetaServerConsoleService> get(List<String> dcNames);
	MetaServerConsoleService get(String dcName);
}
