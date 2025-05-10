package com.ctrip.xpipe.api.utils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * @author wenchao.meng
 *
 * Jan 5, 2017
 */
public interface ControllableFile extends Closeable{
	
	FileChannel getFileChannel() throws IOException;
	
	long size();

	boolean isOpen();

	void setLength(int size) throws IOException;
}
