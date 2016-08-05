package com.ctrip.xpipe.redis.core.entity;

/**
 * @author wenchao.meng
 *
 *         Jul 17, 2016
 */
public interface Keeper extends Redis {

	Long getKeeperContainerId();

	Keeper setId(String id);

	Keeper setIp(String ip);

	Keeper setMaster(String master);

	Keeper setOffset(Long offset);

	Keeper setPort(Integer port);
}
