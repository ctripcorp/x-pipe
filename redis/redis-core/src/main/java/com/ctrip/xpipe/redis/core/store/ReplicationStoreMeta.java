package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @author wenchao.meng
 *
 *         Jun 1, 2016
 */
@SuppressWarnings("serial")
public class ReplicationStoreMeta implements Serializable{

	public static final String EMPTY_REPL_ID = "0000000000000000000000000000000000000000";
	public static final long DEFAULT_BEGIN_OFFSET = 1;
	public static final long DEFAULT_END_OFFSET = DEFAULT_BEGIN_OFFSET - 1;
	public static final long DEFAULT_SECOND_REPLID_OFFSET = -1;
	
	private transient Logger logger = LoggerFactory.getLogger(getClass());
	
	private DefaultEndPoint masterAddress;
	private Long beginOffset = DEFAULT_BEGIN_OFFSET;
	
	private String replId;
	private String replId2;
	private Long secondReplIdOffset = DEFAULT_SECOND_REPLID_OFFSET;
	
	
	private String rdbFile;
	private Long rdbLastOffset;
	private long rdbFileSize;
	private String rdbEofMark;
	private String rdbGtidSet;
	
	// last offset of rdb in keeper coordinate
	private String cmdFilePrefix;

	private KeeperState keeperState;
	private String keeperRunid;


	//these fieled are to be deleted after upgrade to new version
	@Deprecated
	private Long rdbLastKeeperOffset;
	@Deprecated
	private Long keeperBeginOffset;
	

	public ReplicationStoreMeta() {

	}

	public ReplicationStoreMeta(ReplicationStoreMeta proto) {
		this.masterAddress = proto.masterAddress;
		this.beginOffset = proto.beginOffset;
		
		this.replId = proto.replId;
		this.replId2 = proto.replId2;
		this.secondReplIdOffset = proto.secondReplIdOffset;
		
		this.rdbFile = proto.rdbFile;
		this.rdbLastOffset = proto.rdbLastOffset;
		
		this.rdbFileSize = proto.rdbFileSize;
		this.rdbEofMark = proto.rdbEofMark;
		this.rdbGtidSet = proto.rdbGtidSet;
		
		this.cmdFilePrefix = proto.cmdFilePrefix;
		this.keeperState = proto.keeperState;
		this.keeperRunid = proto.keeperRunid;
	}
	
	public long getRdbFileSize() {
		return rdbFileSize;
	}
	
	public String getRdbEofMark() {
		return rdbEofMark;
	}

	public String getRdbGtidSet() {
		return rdbGtidSet;
	}

	public void setRdbGtidSet(String rdbGtidSet) {
		this.rdbGtidSet = rdbGtidSet;
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
		ajastRdbLastOffset();
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
	public String getReplId() {
		return replId;
	}

	public void setReplId(String replId) {
		this.replId = replId;
	}

	public Long getRdbLastOffset() {
		return rdbLastOffset;
	}

	public void setRdbLastOffset(Long rdbLastOffset) {
		this.rdbLastOffset = rdbLastOffset;
	}

	public String getReplId2() {
		return replId2;
	}

	public void setReplId2(String replId2) {
		this.replId2 = replId2;
	}

	public Long getSecondReplIdOffset() {
		return secondReplIdOffset;
	}

	public void setSecondReplIdOffset(Long secondReplIdOffset) {
		this.secondReplIdOffset = secondReplIdOffset;
	}

	public void setCmdFilePrefix(String cmdFilePrefix) {
		this.cmdFilePrefix = cmdFilePrefix;
	}

	public KeeperState getKeeperState() {
		return keeperState;
	}

	public void setKeeperState(KeeperState keeperState) {
		this.keeperState = keeperState;
	}
	
	@Deprecated
	public void setMasterRunid(String masterRunid) {
		this.replId = masterRunid;
	}

	@Deprecated
	public void setRdbLastKeeperOffset(Long rdbLastKeeperOffset) {
		this.rdbLastKeeperOffset = rdbLastKeeperOffset;
		ajastRdbLastOffset();
	}
	
	@Deprecated
	public void setKeeperBeginOffset(Long keeperBeginOffset) {
		this.keeperBeginOffset = keeperBeginOffset;
		ajastRdbLastOffset();
	}

	private void ajastRdbLastOffset() {
		
		if(rdbLastKeeperOffset != null && keeperBeginOffset != null && beginOffset != null ){
			this.rdbLastOffset = beginOffset + (rdbLastKeeperOffset - keeperBeginOffset);
			logger.info("[ajastRdbLastOffset]begin:{}, keeperBegin:{}, rdbLastKeeper:{} ===> rdbLast:{}", beginOffset, keeperBeginOffset, rdbLastKeeperOffset, rdbLastOffset);
		}
	}

	@Override
	public String toString() {
		return "ReplicationStoreMeta [masterAddress=" + masterAddress + ", beginOffset=" + beginOffset 
				+ ", replid=" + replId + ",replid2=" + replId2 + ", secondReplIdOffset=" + secondReplIdOffset  
				+ ", rdbFile=" + rdbFile + ", rdbLastOffset=" + rdbLastOffset + ", rdbFileSize=" + rdbFileSize + ", "+ ", rdbFileEofMark:" + rdbEofMark + 
				", cmdFilePrefix=" + cmdFilePrefix + 
				", keeperState=" + keeperState +", keeperRunid=" + keeperRunid;
	}

	@Override
	public boolean equals(Object obj) {
		return EqualsBuilder.reflectionEquals(this, obj, "rdbLastKeeperOffset", "keeperBeginOffset");
	}
	
	@Override
	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this, false);
	}
}
