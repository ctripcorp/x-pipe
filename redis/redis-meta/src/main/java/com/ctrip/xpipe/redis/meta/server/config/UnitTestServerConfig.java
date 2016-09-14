package com.ctrip.xpipe.redis.meta.server.config;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.utils.IpUtils;

/**
 * @author wenchao.meng
 *
 * Aug 9, 2016
 */
public class UnitTestServerConfig implements MetaServerConfig{

	private String zkAddress = "localhost:2181";

	private String consoleAddress = "http://localhost:9000";

	private int metaServerId = 1;
	
	private int metaServerPort = 9747;
	
	public UnitTestServerConfig(){
		
	}
	public UnitTestServerConfig(int metaServerId, int metaServerPort) {
		
		this.metaServerId = metaServerId;
		this.metaServerPort = metaServerPort;
	}

	@Override
	public String getZkConnectionString() {
		return zkAddress;
	}

	@Override
	public String getConsoleAddress() {
		return consoleAddress;
	}

	@Override
	public int getMetaRefreshMilli() {
		return 60000;
	}

	@Override
	public int getMetaServerId() {
		return metaServerId;
	}

	@Override
	public int getMetaServerPort() {
		return metaServerPort;
	}

	@Override
	public String getMetaServerIp() {
		return IpUtils.getFistNonLocalIpv4ServerAddress().getHostName();
	}


	@Override
	public int getClusterServersRefreshMilli() {
		return 60000;
	}

	@Override
	public int getSlotRefreshMilli() {
		return 60000;
	}

	@Override
	public int getLeaderCheckMilli() {
		return 60000;
	}

	public void setZkAddress(String zkAddress) {
		this.zkAddress = zkAddress;
	}
	
	public void setMetaServerId(int metaServerId) {
		this.metaServerId = metaServerId;
	}
	
	public void setMetaServerPort(int metaServerPort) {
		this.metaServerPort = metaServerPort;
	}
	
	@Override
	public String toString() {
		return Codec.DEFAULT.encode(this);
	}
}
