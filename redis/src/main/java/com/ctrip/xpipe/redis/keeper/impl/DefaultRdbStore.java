package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultRdbStore implements RdbStore {

	private RandomAccessFile writeFile;

	private File file;

	private FileChannel channel;

	private AtomicBoolean writeDone = new AtomicBoolean(false);

	public DefaultRdbStore(File file) throws IOException {
		this.file = file;
		writeFile = new RandomAccessFile(file, "rw");
		channel = writeFile.getChannel();
	}

	@Override
	public int write(ByteBuffer buf) throws IOException {
		return channel.write(buf);
	}

	@Override
	public void endWrite() throws IOException {
		writeFile.close();
		writeDone.set(true);
	}

	@Override
	public RandomAccessFile getRdbFile() throws IOException {
		return new RandomAccessFile(file, "r");
	}

	@Override
	public boolean isWriteDone() {
		return writeDone.get();
	}

}
