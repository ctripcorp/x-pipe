package com.ctrip.xpipe.redis.core.metaserver;

import java.util.List;

public interface MetaServerLocator {

	List<String> getMetaServerList();

}
