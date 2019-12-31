package com.ctrip.xpipe.redis.keeper.config;

/**
 * @author wenchao.meng
 *
 * Aug 22, 2016
 */
public class TestKeeperContainerConfig implements KeeperContainerConfig{
	
	private String replicationStoreDir;
	private String metaServerUrl = System.getProperty("metaServerUrl", "http://localhost:9747");
	
	public TestKeeperContainerConfig(String replicationStoreDir) {
		this.replicationStoreDir = replicationStoreDir;
	}

	@Override
	public String getReplicationStoreDir() {
		return replicationStoreDir;
	}
	
	public void setMetaServerUrl(String metaServerUrl) {
		this.metaServerUrl = metaServerUrl;
	}
	
	public void setReplicationStoreDir(String replicationStoreDir) {
		this.replicationStoreDir = replicationStoreDir;
	}
}
