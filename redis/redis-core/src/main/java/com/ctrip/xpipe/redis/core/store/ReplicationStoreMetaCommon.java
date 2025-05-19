package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.meta.KeeperState;

import java.io.Serializable;

@SuppressWarnings("serial")
public class ReplicationStoreMetaCommon implements Serializable {
	public static final String EMPTY_REPL_ID = "0000000000000000000000000000000000000000";
	public static final long DEFAULT_BEGIN_OFFSET = 1;
	public static final long DEFAULT_END_OFFSET = DEFAULT_BEGIN_OFFSET - 1;
	public static final long DEFAULT_SECOND_REPLID_OFFSET = -1;

	protected DefaultEndPoint masterAddress;

	protected String rdbFile;
	protected long rdbFileSize;
	protected String rdbEofMark;
	protected String rdbGtidSet;

	protected String rordbFile;
	protected long rordbFileSize;
	protected String rordbEofMark;
	protected String rordbGtidSet;

	protected String cmdFilePrefix;

	protected KeeperState keeperState;
	protected String keeperRunid;

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

	public KeeperState getKeeperState() {
		return keeperState;
	}

	public void setKeeperState(KeeperState keeperState) {
		this.keeperState = keeperState;
	}

	public String getRordbFile() {
		return rordbFile;
	}

	public void setRordbFile(String rordbFile) {
		this.rordbFile = rordbFile;
	}

	public long getRordbFileSize() {
		return rordbFileSize;
	}

	public void setRordbFileSize(long rordbFileSize) {
		this.rordbFileSize = rordbFileSize;
	}

	public String getRordbEofMark() {
		return rordbEofMark;
	}

	public void setRordbEofMark(String rordbEofMark) {
		this.rordbEofMark = rordbEofMark;
	}

	public String getRordbGtidSet() {
		return rordbGtidSet;
	}

	public void setRordbGtidSet(String rordbGtidSet) {
		this.rordbGtidSet = rordbGtidSet;
	}

}
