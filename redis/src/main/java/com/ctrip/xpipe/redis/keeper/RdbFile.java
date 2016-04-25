package com.ctrip.xpipe.redis.keeper;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * @author wenchao.meng
 *
 *         2016年4月25日 上午11:54:48
 */
public interface RdbFile {

	FileChannel getRdbFile();

	/**
	 * corresponding masteroffset: +FULLRESYNC masterif masteroffset
	 * 
	 * @return
	 */
	long getRdboffset();

	void close() throws IOException;

}
