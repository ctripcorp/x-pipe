package com.ctrip.xpipe.redis.core.protocal.pojo;

import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author wenchao.meng
 *
 * Nov 11, 2016
 */
public abstract class AbstractRole implements Role{
	
	protected SERVER_ROLE serverRole;
	
	@Override
	public SERVER_ROLE getServerRole() {
		return serverRole;
	}
	
	protected void expectedLen(Object[] payload, int len) {
		
		if(payload.length != len){
			throw new IllegalArgumentException("argument length expected " + len + ", but :" +  payload.length + StringUtil.join(",", payload));
		}
	}
}
