package com.ctrip.xpipe.redis.keeper.store;

import java.io.File;
import java.io.IOException;

import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;


/**
 * @author wenchao.meng
 *
 * Aug 26, 2016
 */
public class DefaultDumpedRdbStore extends DefaultRdbStore implements DumpedRdbStore{
	
	private long masterOffset;

	public DefaultDumpedRdbStore(File file) throws IOException {
		super(file, -1, null);
	}

	@Override
	public long getMasterOffset() {
		return this.masterOffset;
	}

	@Override
	public void setMasterOffset(long masterOffset) {
		this.masterOffset = masterOffset;
	}

	@Override
	public EofType getEofType() {
		return this.eofType;
	}

	
	@Override
	public void setEofType(EofType eofType) {
		this.eofType = eofType;
	}

	@Override
	public File getRdbFile() {
		return file;
	}

	@Override
	public void setRdbLastKeeperOffset(long rdbLastKeeperOffset){
		this.rdbLastKeeperOffset = rdbLastKeeperOffset;
	}
	
	@Override
	public String toString() {
		return String.format("masterOffset:%d,%s", masterOffset, super.toString());
	}
}
