package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.keeper.KeeperMeta;

/**
 * @author wenchao.meng
 *
 * May 23, 2016
 */
public class DefaultKeeperMeta implements KeeperMeta{
	
	private int keeperPort;
	
	private String keeperRunid;
	
	private String keeperName;
	
	public DefaultKeeperMeta(int keeperPort, String keeperRunid, String keeperName) {
		
		this.keeperPort = keeperPort;
		this.keeperRunid = keeperRunid;
		this.keeperName = keeperName;
	}
	
	

	@Override
	public int getKeeperPort() {
		
		return keeperPort;
	}

	@Override
	public String getKeeperRunid() {
		
		return keeperRunid;
	}

	@Override
	public String getKeeperName() {
		
		return keeperName;
	}

	

}
