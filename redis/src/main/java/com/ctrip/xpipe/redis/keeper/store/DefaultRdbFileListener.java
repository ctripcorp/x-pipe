package com.ctrip.xpipe.redis.keeper.store;

import java.nio.channels.FileChannel;

import com.ctrip.xpipe.redis.keeper.RdbFileListener;

/**
 * @author wenchao.meng
 *
 * 2016年5月9日 下午5:31:00
 */
public class DefaultRdbFileListener implements RdbFileListener{

	@Override
	public void onFileData(FileChannel fileChannel, long pos, long len) {
		// TODO Auto-generated method stub
		
	}

}
