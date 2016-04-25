package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import io.netty.buffer.ByteBuf;

public class DefaultCommandStore implements CommandStore {

	private File file;

	private RandomAccessFile writeFile;

	private FileChannel channel;

	public DefaultCommandStore(File file) throws IOException {
		this.file = file;
		writeFile = new RandomAccessFile(file, "rw");
		channel = writeFile.getChannel();
		// append to file
		channel.position(channel.size());
	}

	@Override
	public int appendCommands(ByteBuf byteBuf) throws IOException {
		int wrote = 0;
		ByteBuffer[] buffers = byteBuf.nioBuffers();
		if (buffers != null) {
			for (ByteBuffer buf : buffers) {
				wrote += channel.write(buf);
			}
		}

		return wrote;
	}

	@Override
	public CommandReader beginRead(long startOffset) throws IOException {
		return new DefaultCommandReader(file, startOffset);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
