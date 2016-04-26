package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.RdbFile;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;

import io.netty.buffer.ByteBuf;

public class DefaultReplicationStore implements ReplicationStore {

	private File baseDir;

	public DefaultReplicationStore(File baseDir) {
		this.baseDir = baseDir;
	}

	private RdbStore rdbStore;

	private CommandStore cmdStore;

	private ConcurrentMap<CommandsListener, CommandNotifier> cmdListeners = new ConcurrentHashMap<>();

	private String masterRunid;

	private long beginOffset;

	private long endOffset;

	@Override
	public void close() throws IOException {
		// TODO
	}

	@Override
	public void beginRdb(String masterRunid, long masterOffset) throws IOException {
		this.masterRunid = masterRunid;
		this.beginOffset = masterOffset + 1;
		this.endOffset = masterOffset;

		baseDir.mkdirs();
		// TODO
		rdbStore = new DefaultRdbStore(new File(baseDir, masterRunid), beginOffset);
		cmdStore = new DefaultCommandStore(new File(baseDir, masterRunid + "_cmd"));
	}

	@Override
	public int writeRdb(ByteBuf byteBuffer) throws IOException {
		// TODO ByteBuf to ByteBuffer correct?
		int wrote = 0;
		ByteBuffer[] bufs = byteBuffer.nioBuffers();
		if (bufs != null) {
			for (ByteBuffer buf : bufs) {
				wrote += rdbStore.write(buf);
			}
		}

		return wrote;
	}

	@Override
	public void endRdb() throws IOException {
		rdbStore.endWrite();
	}

	@Override
	public String getMasterRunid() {
		return masterRunid;
	}

	@Override
	public RdbFile getRdbFile() throws IOException {
		return rdbStore.getRdbFile();
	}

	@Override
	public int appendCommands(ByteBuf byteBuf) throws IOException {
		int wrote = cmdStore.appendCommands(byteBuf);
		endOffset += wrote;

		return wrote;
	}

	@Override
	public void addCommandsListener(long offset, CommandsListener commandsListener) throws IOException {
		CommandNotifier notifier = new DefaultCommandNotifier();
		notifier.start(cmdStore, offset - beginOffset, commandsListener);
		cmdListeners.put(commandsListener, notifier);
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
