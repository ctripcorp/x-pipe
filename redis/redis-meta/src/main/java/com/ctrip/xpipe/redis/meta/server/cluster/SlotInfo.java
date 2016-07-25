package com.ctrip.xpipe.redis.meta.server.cluster;

/**
 * @author wenchao.meng
 *
 * Jul 25, 2016
 */
public class SlotInfo {
	
	private int serverId;
	
	public SlotInfo(int serverId){
		this.serverId = serverId;
	}

	public int getServerId() {
		return serverId;
	}

	public void setServerId(int serverId) {
		this.serverId = serverId;
	}
	

}
