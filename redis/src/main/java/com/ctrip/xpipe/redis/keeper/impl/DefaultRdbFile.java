package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import com.ctrip.xpipe.redis.keeper.RdbFile;

public class DefaultRdbFile implements RdbFile {

	private RandomAccessFile file;

	private FileChannel channel;

	private long offset;

	public DefaultRdbFile(File file, long offset) throws IOException {
		this.file = new RandomAccessFile(file, "r");
		channel = this.file.getChannel();
		this.offset = offset;
	}

	@Override
	public FileChannel getRdbFile() {
		return channel;
	}

	@Override
	public long getRdboffset() {
		return offset;
	}

	@Override
	public void close() throws IOException {
		file.close();
	}

}
