package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * @author wenchao.meng
 *
 * Jun 1, 2016
 */
public class ReplicationStoreMeta {
	
	private String masterRunid;
	private Endpoint masterAddress;
	private long beginOffset;
	private long keeperBeginOffset;
	private boolean active;
	
	
	public ReplicationStoreMeta(){
		
	}
	
	public ReplicationStoreMeta(String masterRunid, Endpoint masterAddress, long beginOffset, long keeperBeginOffset, boolean active){
		
		this.masterRunid = masterRunid;
		this.masterAddress = masterAddress;
		this.beginOffset = beginOffset;
		this.keeperBeginOffset = keeperBeginOffset;
		this.active = active;
	}
	
	
	public String getMasterRunid() {
		return masterRunid;
	}
	public void setMasterRunid(String masterRunid) {
		this.masterRunid = masterRunid;
	}
	public Endpoint getMasterAddress() {
		return masterAddress;
	}
	public void setMasterAddress(Endpoint masterAddress) {
		this.masterAddress = masterAddress;
	}
	public long getBeginOffset() {
		return beginOffset;
	}
	public void setBeginOffset(long beginOffset) {
		this.beginOffset = beginOffset;
	}
	public long getKeeperBeginOffset() {
		return keeperBeginOffset;
	}
	public void setKeeperBeginOffset(long keeperBeginOffset) {
		this.keeperBeginOffset = keeperBeginOffset;
	}
	public boolean isActive() {
		return active;
	}
	public void setActive(boolean active) {
		this.active = active;
	}
}
