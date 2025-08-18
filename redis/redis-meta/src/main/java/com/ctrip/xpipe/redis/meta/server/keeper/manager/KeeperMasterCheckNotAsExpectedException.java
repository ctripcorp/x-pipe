package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.redis.meta.server.exception.MetaServerException;
import com.ctrip.xpipe.redis.meta.server.job.KEEPER_ALERT;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
public class KeeperMasterCheckNotAsExpectedException extends MetaServerException{

	private static final long serialVersionUID = 1L;
	private KEEPER_ALERT alert;

	public KeeperMasterCheckNotAsExpectedException(String ip, int port, KEEPER_ALERT alert) {
		super(String.format("keeperMaster:%s:%d, error:%s", ip, port, alert.getDesc()));
		setOnlyLogMessage(true);
		this.alert = alert;
	}

	public KEEPER_ALERT getAlert() {
		return alert;
	}

	public KeeperMasterCheckNotAsExpectedException(String ip, int port, Throwable th){
		super(String.format("keeperMaster:%s:%d, exception happen", ip, port), th);
	}

}
