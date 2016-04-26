package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.RdbFile;
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

	public DefaultReplicationStore(File baseDir) throws IOException {
		this.baseDir = baseDir;
		loadMeta();
	}

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
		saveMeta();

		// TODO file naming
		rdbStore = new DefaultRdbStore(rdbFileOf(masterRunid), beginOffset);
		cmdStore = new DefaultCommandStore(cmdFileOf(masterRunid));
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
