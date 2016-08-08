package com.ctrip.xpipe.redis.core.store;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * @author wenchao.meng
 *
 * 2016年5月9日 下午5:28:47
 */
public interface RdbFileListener {
	
	
	/**
	 * @param rdbFileSize
	 * @param rdbFileOffset
	 */
	void setRdbFileInfo(long rdbFileSize, long rdbFileKeeperOffset);
	
	/**
	 * 
	 * @param fileChannel
	 * @param pos
	 * @param len  when len == -1, that means file has ended
	 * @throws IOException 
	 */
	void onFileData(FileChannel fileChannel, long pos, long len) throws IOException;
	
	/**
	 * @return
	 */
   boolean isOpen();
   
   /**
    * meet exception while reading rdb
	 * @param e
	 */
   void exception(Exception e);
   
   /**
    * called before any other methods to provide a hook to do some initialization
    */
   void beforeFileData();
   
}
