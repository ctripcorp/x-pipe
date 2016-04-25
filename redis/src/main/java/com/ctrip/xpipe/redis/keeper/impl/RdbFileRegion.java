package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import io.netty.channel.FileRegion;
import io.netty.util.ReferenceCounted;

public class RdbFileRegion implements FileRegion {

	private RandomAccessFile rdbFile;

	public RdbFileRegion(File rdbFile) throws IOException {
		this.rdbFile = new RandomAccessFile(rdbFile, "r");
	}

	@Override
	public int refCnt() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ReferenceCounted retain() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ReferenceCounted retain(int increment) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean release() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean release(int decrement) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long position() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long transfered() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long count() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long transferTo(WritableByteChannel target, long position) throws IOException {
		FileChannel channel = rdbFile.getChannel();
		long total = channel.size() - position;
		long transferred = 0;
		while (transferred < total) {
			// TODO smaller buf size?
			transferred += channel.transferTo(position, Integer.MAX_VALUE, target);
			position += transferred;
		}
		return transferred;
	}

}
