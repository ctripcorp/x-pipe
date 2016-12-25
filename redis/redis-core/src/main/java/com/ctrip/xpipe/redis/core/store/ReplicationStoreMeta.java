package com.ctrip.xpipe.redis.core.store;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.meta.KeeperState;

/**
 * @author wenchao.meng
 *
 *         Jun 1, 2016
 */
@SuppressWarnings("serial")
public class ReplicationStoreMeta implements Serializable{

	public static final int DEFAULT_KEEPER_BEGIN_OFFSET = 2;

	private String masterRunid;
	private DefaultEndPoint masterAddress;
	private Long beginOffset;
	private String rdbFile;
	
	private long rdbFileSize;
	private String rdbEofMark;
	
	// last offset of rdb in keeper coordinate
	private Long rdbLastKeeperOffset;
	private String cmdFilePrefix;

	private KeeperState keeperState;

	private long keeperBeginOffset = DEFAULT_KEEPER_BEGIN_OFFSET;
	private String keeperRunid;
	
	private DefaultEndPoint activeKeeperAddress;


	public ReplicationStoreMeta() {

	}

	public ReplicationStoreMeta(ReplicationStoreMeta proto) {
		this.masterRunid = proto.masterRunid;
		this.masterAddress = proto.masterAddress;
		this.beginOffset = proto.beginOffset;
		this.keeperState = proto.keeperState;
		this.rdbFile = proto.rdbFile;
		
		this.rdbFileSize = proto.rdbFileSize;
		this.rdbEofMark = proto.rdbEofMark;
		
		this.rdbLastKeeperOffset = proto.rdbLastKeeperOffset;
		this.cmdFilePrefix = proto.cmdFilePrefix;

		this.keeperBeginOffset = proto.keeperBeginOffset;
		this.keeperRunid = proto.keeperRunid;
		
		proto.activeKeeperAddress = activeKeeperAddress;
	}
	
	public long getRdbFileSize() {
		return rdbFileSize;
	}
	
	public String getRdbEofMark() {
		return rdbEofMark;
	}

	public void setRdbFileSize(long rdbFileSize) {
		this.rdbFileSize = rdbFileSize;
	}
	
	public void setRdbEofMark(String rdbEofMark) {
		this.rdbEofMark = rdbEofMark;
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

	public Long getBeginOffset() {
		return beginOffset;
	}

	public void setBeginOffset(Long beginOffset) {
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
				+ ", rdbFileSize=" + rdbFileSize + "rdbFileEofMark:" + rdbEofMark + ", rdbLastKeeperOffset=" + rdbLastKeeperOffset + ", cmdFilePrefix=" + cmdFilePrefix + ", keeperState=" + keeperState
				+ ", keeperBeginOffset=" + keeperBeginOffset + ", keeperRunid=" + keeperRunid + ", activeKeeperAddress:" + activeKeeperAddress + "]";
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

	public Long getRdbLastKeeperOffset() {
		return rdbLastKeeperOffset;
	}

	public void setRdbLastKeeperOffset(Long rdbLastKeeperOffset) {
		this.rdbLastKeeperOffset = rdbLastKeeperOffset;
	}
	
	public KeeperState getKeeperState() {
		return keeperState;
	}

	public void setKeeperState(KeeperState keeperState) {
		this.keeperState = keeperState;
	}
	
	@Override
	public boolean equals(Object obj) {
		return EqualsBuilder.reflectionEquals(this, obj);
	}
	
	@Override
	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this);
	}
	
	public DefaultEndPoint getActiveKeeperAddress() {
		return activeKeeperAddress;
	}

	public void setActiveKeeperAddress(DefaultEndPoint activeKeeperAddress) {
		this.activeKeeperAddress = activeKeeperAddress;
	}
}
