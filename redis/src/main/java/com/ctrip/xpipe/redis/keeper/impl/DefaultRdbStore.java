package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultRdbStore implements RdbStore {

	private RandomAccessFile writeFile;

	private File file;

	private FileChannel channel;

	private long rdbFileSize;

	private AtomicReference<Status> status = new AtomicReference<>(Status.Writing);

	public DefaultRdbStore(File file, long rdbFileSize) throws IOException {
		this.file = file;
		this.rdbFileSize = rdbFileSize;
		writeFile = new RandomAccessFile(file, "rw");
		channel = writeFile.getChannel();
	}

	@Override
	public int write(ByteBuffer buf) throws IOException {
		return channel.write(buf);
	}

	@Override
	public void endWrite() throws IOException {
		long actualFileLen = writeFile.length();
		writeFile.close();

		if (actualFileLen == rdbFileSize) {
			status.set(Status.Success);
		} else {
			status.set(Status.Fail);
		}
	}

	@Override
	public RandomAccessFile getRdbFile() throws IOException {
		return new RandomAccessFile(file, "r");
	}

	@Override
	public Status getStatus() {
		return status.get();
	}

}
