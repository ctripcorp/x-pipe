package com.ctrip.xpipe.redis.core.protocal.pojo;

import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;

/**
 * @author wenchao.meng
 *
 * Nov 11, 2016
 */
public class MasterRole extends AbstractRole{
	
	public MasterRole(Object []payload) {
		expectedLen(payload, 3);
		this.serverRole = SERVER_ROLE.of(payload[0].toString());
	}

	
	@Override
	public String toString() {
		return String.format("role:%s", serverRole);
	}
}
