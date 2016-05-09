package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.RdbFileListener;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;

import io.netty.buffer.ByteBuf;

public class DefaultReplicationStore implements ReplicationStore {

	private final static Logger logger = LogManager.getLogger(DefaultReplicationStore.class);

	private static final String MASTER_RUNID = "master.runid";

	private static final String BEGIN_OFFSET = "begin.offset";

	private static final String META_FILE = "meta.properties";

	private File baseDir;

	private RdbStore rdbStore;

	private CommandStore cmdStore;

	private ConcurrentMap<CommandsListener, CommandNotifier> cmdListeners = new ConcurrentHashMap<>();

	private String masterRunid;

	private long beginOffset;

	private long endOffset;

	private int cmdFileSize;

	private long rdbFileSize;

	public DefaultReplicationStore(File baseDir, int cmdFileSize) throws IOException {
		this.baseDir = baseDir;
		this.cmdFileSize = cmdFileSize;
		loadMeta();
	}

	@Override
	public void close() throws IOException {
		// TODO
	}

	@Override
	public void beginRdb(String masterRunid, long masterOffset, long rdbFileSize) throws IOException {
		this.masterRunid = masterRunid;
		// TODO save master offset
		this.beginOffset = masterOffset + 1;
		this.endOffset = masterOffset;
		this.rdbFileSize = rdbFileSize;

		baseDir.mkdirs();
		saveMeta();

		// TODO file naming
		rdbStore = new DefaultRdbStore(rdbFileOf(masterRunid));
		cmdStore = new DefaultCommandStore(cmdFileOf(masterRunid), cmdFileSize);
	}

	private File cmdFileOf(String masterRunid) {
		return new File(baseDir, masterRunid + "_cmd");
	}

	private File rdbFileOf(String masterRunid) {
		return new File(baseDir, masterRunid);
	}

	private void saveMeta() throws IOException {
		Properties props = new Properties();
		props.setProperty(MASTER_RUNID, masterRunid);
		props.setProperty(BEGIN_OFFSET, Long.toString(beginOffset));

		// TODO file naming
		try (FileWriter metaWriter = new FileWriter(new File(baseDir, META_FILE))) {
			props.store(metaWriter, null);
		}
	}

	private void loadMeta() throws IOException {
		File metaFile = new File(baseDir, META_FILE);
		if (metaFile.isFile()) {
			try (FileReader reader = new FileReader(metaFile)) {
				Properties props = new Properties();
				props.load(reader);

				masterRunid = props.getProperty(MASTER_RUNID);
				beginOffset = Long.parseLong(props.getProperty(BEGIN_OFFSET));

				File rdbFile = rdbFileOf(masterRunid);
				if (rdbFile.isFile()) {
					endOffset = beginOffset + rdbFile.length() - 1;
				} else {
					endOffset = beginOffset - 1;
				}

				logger.info(String.format("Meta loaded masterRunid=%s, beginOffset=%s, endOffset=%s", masterRunid,
				      beginOffset, endOffset));
			}
		}
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

	@Override
	public void readRdbFile(RdbFileListener rdbFileListener) throws IOException {
		rdbFileListener.setRdbFileInfo(rdbFileSize, beginOffset - 1); // beginOffset - 1 == masteroffset
		try (RandomAccessFile rdbFile = rdbStore.getRdbFile()) {
			try (FileChannel channel = rdbFile.getChannel()) {
				long start = 0;

				while (!rdbStore.isWriteDone()) {
					if (channel.size() > start) {
						rdbFileListener.onFileData(channel, start, channel.size() - start);
						start = channel.size();
					} else {
						// TODO
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
						}
					}
				}

				rdbFileListener.onFileData(channel, start, -1L);
			}
		}
	}

	@Override
	public long stopReadingRdbFile(RdbFileListener rdbFileListener) {
		// TODO Auto-generated method stub
		return 0;
	}

}
