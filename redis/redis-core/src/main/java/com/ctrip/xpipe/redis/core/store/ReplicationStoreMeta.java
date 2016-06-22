package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;

/**
 * @author wenchao.meng
 *
 *         Jun 1, 2016
 */
public class ReplicationStoreMeta {

	private String masterRunid;
	private DefaultEndPoint masterAddress;
	private long beginOffset;
	private String rdbFile;
	private long rdbFileSize;
	
	private boolean active;

	private long keeperBeginOffset;
	private String keeperRunid;

	
	public ReplicationStoreMeta() {

	}

	public ReplicationStoreMeta(String masterRunid, DefaultEndPoint masterAddress, long beginOffset, long keeperBeginOffset, boolean active) {

		this.masterRunid = masterRunid;
		this.masterAddress = masterAddress;
		this.beginOffset = beginOffset;
		this.keeperBeginOffset = keeperBeginOffset;
		this.active = active;
	}

	public ReplicationStoreMeta(ReplicationStoreMeta proto) {
		this.masterRunid = proto.masterRunid;
		this.masterAddress = proto.masterAddress;
		this.beginOffset = proto.beginOffset;
		this.active = proto.active;
		this.rdbFile = proto.rdbFile;
		this.rdbFileSize = proto.rdbFileSize;
		
		this.keeperBeginOffset = proto.keeperBeginOffset;
		this.keeperRunid = proto.keeperRunid;
	}

	/**
	 * @return the rdbFileSize
	 */
	public long getRdbFileSize() {
		return rdbFileSize;
	}

	/**
	 * @param rdbFileSize
	 *            the rdbFileSize to set
	 */
	public void setRdbFileSize(long rdbFileSize) {
		this.rdbFileSize = rdbFileSize;
	}

	public String getRdbFile() {
		return rdbFile;
	}

	public void setRdbFile(String rdbFile) {
		this.rdbFile = rdbFile;
	}

	public String getMasterRunid() {
		return masterRunid;
	}

	public void setMasterRunid(String masterRunid) {
		this.masterRunid = masterRunid;
	}

	public DefaultEndPoint getMasterAddress() {
		return masterAddress;
	}

	public void setMasterAddress(DefaultEndPoint masterAddress) {
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

	@Override
	public String toString() {
		return "ReplicationStoreMeta [masterRunid=" + masterRunid + ", masterAddress=" + masterAddress + ", beginOffset=" + beginOffset + ", keeperRunid:" + keeperRunid + ", keeperBeginOffset="
				+ keeperBeginOffset + ", active=" + active + ", rdbFile=" + rdbFile + ", rdbFileSize=" + rdbFileSize + "]";
	}

	public String getKeeperRunid() {
		return keeperRunid;
	}

	public void setKeeperRunid(String keeperRunid) {
		this.keeperRunid = keeperRunid;
	}

}
