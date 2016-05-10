package com.ctrip.xpipe.netty;

import java.nio.channels.FileChannel;

import io.netty.channel.DefaultFileRegion;

/**
 * @author wenchao.meng
 *
 * May 10, 2016 11:23:28 AM
 */
public class NotClosableFileRegion extends DefaultFileRegion{

	public NotClosableFileRegion(FileChannel file, long position, long count) {
		super(file, position, count);
	}
	
	
	@Override
	protected void deallocate() {
	}
}
