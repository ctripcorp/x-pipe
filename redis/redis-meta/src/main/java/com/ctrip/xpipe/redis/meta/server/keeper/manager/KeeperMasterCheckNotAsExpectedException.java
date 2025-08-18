package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.redis.meta.server.exception.MetaServerException;

public class KeeperMasterCheckNotAsExpectedException extends MetaServerException{

	private static final long serialVersionUID = 1L;
	private KeeperAlertType alert;

	public KeeperMasterCheckNotAsExpectedException(String ip, int port, KeeperAlertType alert) {
		super(String.format("keeperMaster:%s:%d, error:%s", ip, port, alert.getDesc()));
		setOnlyLogMessage(true);
		this.alert = alert;
	}

	public KeeperAlertType getAlert() {
		return alert;
	}

	public KeeperMasterCheckNotAsExpectedException(String ip, int port, Throwable th){
		super(String.format("keeperMaster:%s:%d, exception happen", ip, port), th);
	}

	public enum KeeperAlertType {
		CHECK_NOT_REDIS("not redis"),
		CHECK_NOT_MASTER("not master"),
		CHECK_MULTI_MASTER("multi master"),
		CHANGE_NOT_FIND("can not find active keeper"),
		COMMAND_FAIL("command fail");

		private String desc;

		KeeperAlertType(String desc){
			this.desc = desc;
		}

		public String getDesc() {
			return desc;
		}

	}

}
