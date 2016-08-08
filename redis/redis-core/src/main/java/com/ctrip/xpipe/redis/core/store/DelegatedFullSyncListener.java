/**
 * 
 */
package com.ctrip.xpipe.redis.core.store;

import java.io.IOException;
import java.nio.channels.FileChannel;

import io.netty.buffer.ByteBuf;

/**
 * @author marsqing
 *
 *         Aug 4, 2016 1:38:32 PM
 */
public class DelegatedFullSyncListener implements FullSyncListener {

	private FullSyncListener delegatee;

	public DelegatedFullSyncListener(FullSyncListener delegatee) {
		this.delegatee = delegatee;
	}

	public void onCommand(ByteBuf byteBuf) {
		delegatee.onCommand(byteBuf);
	}

	public void setRdbFileInfo(long rdbFileSize, long rdbFileKeeperOffset) {
		delegatee.setRdbFileInfo(rdbFileSize, rdbFileKeeperOffset);
	}

	public void beforeCommand() {
		delegatee.beforeCommand();
	}

	public void onFileData(FileChannel fileChannel, long pos, long len) throws IOException {
		delegatee.onFileData(fileChannel, pos, len);
	}

	public boolean isOpen() {
		return delegatee.isOpen();
	}

	public void exception(Exception e) {
		delegatee.exception(e);
	}

	public void beforeFileData() {
		delegatee.beforeFileData();
	}

}
