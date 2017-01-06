package com.ctrip.xpipe.api.utils;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * control the size returned
 * @author wenchao.meng
 *
 * Jan 5, 2017
 */
public interface FileSize {
	
	long getSize(FileChannel fileChannel) throws IOException;

}
