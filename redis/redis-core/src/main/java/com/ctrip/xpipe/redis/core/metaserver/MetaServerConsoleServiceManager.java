package com.ctrip.xpipe.redis.core.metaserver;

/**
 * used for console
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface MetaServerConsoleServiceManager{

	MetaServerConsoleService getOrCreate(String metaServerAddress);

	MetaServerConsoleService getOrCreateFastService(String metaServerAddress);
}
