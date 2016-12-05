package com.ctrip.xpipe.redis.core.store.exception;

import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;

/**
 * @author wenchao.meng
 *
 * Dec 5, 2016
 */
public class BadMetaStoreException extends RedisRuntimeException{

	private static final long serialVersionUID = 1L;

	public BadMetaStoreException(String storeKeeperRunid, String currentKeeperRunid) {
		super(String.format("storeKeeperRunid:%s, current:%s", storeKeeperRunid, currentKeeperRunid));
	}

}
