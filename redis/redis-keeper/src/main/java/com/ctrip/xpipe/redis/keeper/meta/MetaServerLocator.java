package com.ctrip.xpipe.redis.keeper.meta;

import java.util.List;

public interface MetaServerLocator {

	List<String> getMetaServerList();

}
