package com.ctrip.xpipe.redis.meta.server.config;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.zk.ZkConfig;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Aug 9, 2016
 */
public class UnitTestServerConfig implements MetaServerConfig{

	private String zkAddress = "127.0.0.1:2181";

	private String consoleAddress = "http://127.0.0.1:9000";
	
	private String zkNameSpace = ZkConfig.DEFAULT_ZK_NAMESPACE;

	private int metaServerId = 1;
	
	private int metaServerPort = 9747;

	private int waitforOffsetMilli = 1000;

	private int waitForMetaSyncDelayMilli = 0;
	
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
		return "127.0.0.1";
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

	@Override
	public String getZkNameSpace() {
		return zkNameSpace;
	}
	
	public void setZkNameSpace(String zkNameSpace) {
		this.zkNameSpace = zkNameSpace;
	}

	@Override
	public Map<String, DcInfo> getDcInofs() {
		Map<String, DcInfo> result = Maps.newHashMap();
		result.put(FoundationService.DEFAULT.getDataCenter(), new DcInfo("http://localhost:" + metaServerPort));
		return result;
	}

	@Override
	public int getWaitforOffsetMilli() {
		return waitforOffsetMilli;
	}

	@Override
	public boolean validateDomain() {
		return true;
	}

	@Override
	public int getKeeperInfoCheckInterval() {
		return 1;
	}

	@Override
	public int getApplierInfoCheckInterval() {
		return 1;
	}

	public UnitTestServerConfig setWaitforOffsetMilli(int waitforOffsetMilli) {
		this.waitforOffsetMilli = waitforOffsetMilli;
		return this;
	}

	public void setWaitForMetaSyncDelayMilli(int delayMilli) {
		this.waitForMetaSyncDelayMilli = delayMilli;
	}

	@Override
	public int getWaitForMetaSyncDelayMilli() {
		return waitForMetaSyncDelayMilli;
	}

	@Override
	public Set<String> getOwnClusterType() {
		return Collections.singleton(ClusterType.ONE_WAY.toString());
	}

	@Override
	public boolean shouldCorrectPeerMasterPeriodically() {
		return true;
	}

	@Override
	public long getNewMasterCacheTimeoutMilli() {
		return 10L;
	}
}
