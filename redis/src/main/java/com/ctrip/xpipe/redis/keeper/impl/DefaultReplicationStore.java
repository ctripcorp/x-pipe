package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.RdbFile;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.FileRegion;

public class DefaultReplicationStore implements ReplicationStore {

	private File baseDir = new File(System.getProperty("user.home"), "tmp/xpipe");

	private RandomAccessFile rdbAccessFile;

	private File rdbFile;

	private IBigQueue cmdQueue;

	// TODO thread safe
	private List<CommandsListener> cmdListeners = new ArrayList<>();

	private String masterRunid;

	private long beginOffset;

	private long endOffset;

	@Override
	public void close() throws IOException {
		cmdQueue.close();
	}

	@Override
	public void beginRdb(String masterRunid, long masterOffset) throws IOException {
		this.masterRunid = masterRunid;
		this.beginOffset = masterOffset + 1;
		this.endOffset = masterOffset;

		baseDir.mkdirs();
		rdbFile = new File(baseDir, masterRunid);
		rdbAccessFile = new RandomAccessFile(rdbFile, "rw");

		// TODO
		cmdQueue = new BigQueueImpl(new File(baseDir, "bigqueue").getCanonicalPath(), masterRunid);

		new Thread() {
			public void run() {
				while (true) {
					try {
						byte[] cmd = cmdQueue.dequeue();
						if (cmd == null) {
							Thread.sleep(100);
						} else {
							for (CommandsListener listener : cmdListeners) {
								// TODO
								listener.onCommand(Unpooled.wrappedBuffer(cmd));
							}
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}.start();
	}

	@Override
	public int writeRdb(ByteBuf byteBuffer) throws IOException {
		// TODO ByteBuf to ByteBuffer correct?
		int wrote = 0;
		ByteBuffer[] bufs = byteBuffer.nioBuffers();
		if (bufs != null) {
			for (ByteBuffer buf : bufs) {
				wrote += rdbAccessFile.getChannel().write(buf);
			}
		}

		return wrote;
	}

	@Override
	public void endRdb() throws IOException {
		rdbAccessFile.close();
	}

	@Override
	public String getMasterRunid() {
		return masterRunid;
	}

	@Override
	public RdbFile getRdbFile() throws IOException {
		return null;
	}

	@Override
	public int appendCommands(ByteBuf byteBuf) throws IOException {
		// TODO transfer to queue
		byte[] bytes = new byte[byteBuf.readableBytes()];
		byteBuf.getBytes(0, bytes);
		cmdQueue.enqueue(bytes);
		endOffset += bytes.length;

		return bytes.length;
	}

	@Override
	public void addCommandsListener(long offset, CommandsListener commandsListener) {
		cmdListeners.add(commandsListener);
	}

	@Override
	public void removeCommandsListener(CommandsListener commandsListener) {
		cmdListeners.remove(commandsListener);
	}

	@Override
	public long beginOffset() {
		return beginOffset;
	}

	@Override
	public long endOffset() {
		return endOffset;
	}

	@Override
	public void delete() {
		// TODO Auto-generated method stub

	}

}
