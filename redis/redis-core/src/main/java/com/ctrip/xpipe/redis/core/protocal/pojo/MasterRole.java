package com.ctrip.xpipe.redis.core.protocal.pojo;

import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * Nov 11, 2016
 */
public class MasterRole extends AbstractRole{

	private long offset;

	public MasterRole(Object []payload) {
		expectedLen(payload, 3);
		this.serverRole = SERVER_ROLE.of(payload[0].toString());
	}

	public MasterRole(){
		serverRole = SERVER_ROLE.MASTER;
	}

	@Override
	public ByteBuf format() {
		Object[] tmp = new Object[] { serverRole.toString(), offset, new Object[0]};
		return new ArrayParser(tmp).format();
	}



	@Override
	public String toString() {
		return String.format("role:%s", serverRole);
	}



}
