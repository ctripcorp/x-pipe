package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.ctrip.xpipe.redis.keeper.RdbFile;

public class DefaultRdbStore implements RdbStore {

	private RandomAccessFile writeFile;

	private File file;

	private FileChannel channel;

	private long beginOffset;

	public DefaultRdbStore(File file, long beginOffset) throws IOException {
		this.file = file;
		this.beginOffset = beginOffset;
		writeFile = new RandomAccessFile(file, "rw");
		channel = writeFile.getChannel();
	}

	@Override
	public int write(ByteBuffer buf) throws IOException {
		return channel.write(buf, 0);
	}

	@Override
	public void endWrite() throws IOException {
		writeFile.close();
	}

	@Override
	public RdbFile getRdbFile() throws IOException {
		return new DefaultRdbFile(file, beginOffset);
	}

}
