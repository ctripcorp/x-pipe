package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.meta.server.exception.MetaServerException;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
public class KeeperMasterStateNotAsExpectedException extends MetaServerException{
	
	private static final long serialVersionUID = 1L;
		
	private SlaveRole keeperRole;

	public KeeperMasterStateNotAsExpectedException(KeeperMeta keeperMeta, SlaveRole role, MASTER_STATE expected){
		super(String.format("keeper:%s:%d, current:%s, expected:%s", keeperMeta.getIp(), keeperMeta.getPort(), role.getMasterState(), expected));
		this.keeperRole = role;
		setOnlyLogMessage(true);
	}

	public KeeperMasterStateNotAsExpectedException(KeeperMeta keeperMeta, Throwable th){
		super(String.format("keeper:%s:%d, exception happen", keeperMeta.getIp(), keeperMeta.getPort()), th);
	}

	public SlaveRole getKeeperRole() {
		return keeperRole;
	}
}
