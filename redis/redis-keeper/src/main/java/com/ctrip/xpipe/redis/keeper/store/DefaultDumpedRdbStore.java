package com.ctrip.xpipe.redis.keeper.store;

import java.io.File;
import java.io.IOException;

import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;


/**
 * @author wenchao.meng
 *
 * Aug 26, 2016
 */
public class DefaultDumpedRdbStore extends DefaultRdbStore implements DumpedRdbStore{
	
	private long masterOffset;

	public DefaultDumpedRdbStore(File file) throws IOException {
		super(file, -1, -1);
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
	public long getRdbFileSize() {
		return this.rdbFileSize;
	}

	@Override
	public void setRdbFileSize(long rdbFileSize) {
		this.rdbFileSize = rdbFileSize;
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
