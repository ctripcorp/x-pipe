package com.ctrip.xpipe.redis.core.entity;

/**
 * @author wenchao.meng
 *
 * Jul 17, 2016
 */
public interface Redis {

	   String getId();

	   String getIp();

	   String getMaster();

	   Long getOffset();

	   Integer getPort();

	   Redis setId(String id);

	   Redis setIp(String ip);

	   Redis setMaster(String master);

	   Redis setOffset(Long offset);

	   Redis setPort(Integer port);
}
