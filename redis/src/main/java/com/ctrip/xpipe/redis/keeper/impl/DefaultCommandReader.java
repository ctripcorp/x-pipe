package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DefaultCommandReader implements CommandReader {

	private RandomAccessFile readFile;
	
	private FileChannel channel;

	public DefaultCommandReader(File file, long startOffset) throws IOException {
		readFile = new RandomAccessFile(file, "r");
		channel = readFile.getChannel();
		channel.position(startOffset);
	}

	@Override
	public void close() throws IOException {
		readFile.close();
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		return channel.read(dst);
	}

}
