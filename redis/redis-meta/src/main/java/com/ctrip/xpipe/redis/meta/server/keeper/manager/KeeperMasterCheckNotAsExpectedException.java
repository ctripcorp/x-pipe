package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.redis.meta.server.exception.MetaServerException;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
public class KeeperMasterCheckNotAsExpectedException extends MetaServerException{

	private static final long serialVersionUID = 1L;

	public KeeperMasterCheckNotAsExpectedException(String ip, int port, String message){
		super(String.format("keeperMaster:%s:%d, error:%s", ip, port, message));
		setOnlyLogMessage(true);
	}

	public KeeperMasterCheckNotAsExpectedException(String ip, int port, Throwable th){
		super(String.format("keeperMaster:%s:%d, exception happen", ip, port), th);
	}

}
