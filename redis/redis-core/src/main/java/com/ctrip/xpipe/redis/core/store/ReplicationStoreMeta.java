package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;

/**
 * @author wenchao.meng
 *
 *         Jun 1, 2016
 */
public class ReplicationStoreMeta {

	public static final int DEFAULT_KEEPER_BEGIN_OFFSET = 2;

	private String masterRunid;
	private DefaultEndPoint masterAddress;
	private long beginOffset;
	private String rdbFile;
	private long rdbFileSize;
	// last offset of rdb in keeper coordinate
	private long rdbLastKeeperOffset;
	private String cmdFilePrefix;

	private boolean active;

	private long keeperBeginOffset = DEFAULT_KEEPER_BEGIN_OFFSET;
	private String keeperRunid;

	public ReplicationStoreMeta() {

	}

	public ReplicationStoreMeta(ReplicationStoreMeta proto) {
		this.masterRunid = proto.masterRunid;
		this.masterAddress = proto.masterAddress;
		this.beginOffset = proto.beginOffset;
		this.active = proto.active;
		this.rdbFile = proto.rdbFile;
		this.rdbFileSize = proto.rdbFileSize;
		this.rdbLastKeeperOffset = proto.rdbLastKeeperOffset;
		this.cmdFilePrefix = proto.cmdFilePrefix;

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

	@Override
	public String toString() {
		return "ReplicationStoreMeta [masterRunid=" + masterRunid + ", masterAddress=" + masterAddress + ", beginOffset=" + beginOffset + ", rdbFile=" + rdbFile
				+ ", rdbFileSize=" + rdbFileSize + ", rdbLastKeeperOffset=" + rdbLastKeeperOffset + ", cmdFilePrefix=" + cmdFilePrefix + ", active=" + active
				+ ", keeperBeginOffset=" + keeperBeginOffset + ", keeperRunid=" + keeperRunid + "]";
	}

	public String getKeeperRunid() {
		return keeperRunid;
	}

	public void setKeeperRunid(String keeperRunid) {
		this.keeperRunid = keeperRunid;
	}

	public String getCmdFilePrefix() {
		return cmdFilePrefix;
	}

	public void setCmdFilePrefix(String cmdFilePrefix) {
		this.cmdFilePrefix = cmdFilePrefix;
	}

	public long getRdbLastKeeperOffset() {
		return rdbLastKeeperOffset;
	}

	public void setRdbLastKeeperOffset(long rdbLastKeeperOffset) {
		this.rdbLastKeeperOffset = rdbLastKeeperOffset;
	}
	
}
