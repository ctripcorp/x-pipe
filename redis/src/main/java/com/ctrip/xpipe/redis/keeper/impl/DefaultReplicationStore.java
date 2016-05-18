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

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.RdbFileListener;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.impl.RdbStore.Status;

import io.netty.buffer.ByteBuf;

public class DefaultReplicationStore implements ReplicationStore {

	private final static Logger logger = LogManager.getLogger(DefaultReplicationStore.class);

	private static final String MASTER_RUNID = "master.runid";
	
	private static final String MASTER_ADDRESS = "master.address";

	private static final String BEGIN_OFFSET = "begin.offset";

	private static final String META_FILE = "meta.properties";

	private File baseDir;

	private RdbStore rdbStore;

	private RandomAccessFile rdbReadFile;

	private CommandStore cmdStore;

	private ConcurrentMap<CommandsListener, CommandNotifier> cmdListeners = new ConcurrentHashMap<>();

	private String masterRunid;
	
	private Endpoint masterEndpoint;

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
		rdbStore = new DefaultRdbStore(rdbFileOf(masterRunid), rdbFileSize);
		rdbReadFile = rdbStore.getRdbFile();
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
		props.setProperty(MASTER_ADDRESS, masterEndpoint.toString());

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
				masterEndpoint = new DefaultEndPoint(props.getProperty(MASTER_ADDRESS));

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
	public void readRdbFile(final RdbFileListener rdbFileListener) throws IOException {
		// TODO use "selector" to reduce thread
		new Thread() {
			public void run() {
				while (rdbReadFile == null) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
					}
				}

				try {
					doReadRdbFile(rdbFileListener);
				} catch (Exception e) {
					logger.error("Error read rdb file", e);
				}
			}
		}.start();
	}

	private void doReadRdbFile(RdbFileListener rdbFileListener) throws IOException {
		rdbFileListener.setRdbFileInfo(rdbFileSize, beginOffset - 1); // beginOffset - 1 == masteroffset
		FileChannel channel = rdbReadFile.getChannel();
		long start = 0;

		while (!rdbFileListener.isStop() && (isRdbWriting(rdbStore.getStatus()) || start < channel.size())) {
			if (channel.size() > start) {
				long end = channel.size();
				rdbFileListener.onFileData(channel, start, end - start);
				start = end;
			} else {
				// TODO
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
			}
		}

		switch (rdbStore.getStatus()) {
		case Success:
			rdbFileListener.onFileData(channel, start, -1L);
			break;

		case Fail:
			// TODO
			rdbFileListener.exception(new Exception(""));
			break;

		default:
			break;
		}
	}

	/**
	 * @param status
	 * @return
	 */
	private boolean isRdbWriting(Status status) {
		return status != Status.Fail && status != Status.Success;
	}

	@Override
	public void masterChanged(Endpoint newMasterEndpoint, String newMasterRunid, long offsetDelta) {
		
		this.setMasterAddress(newMasterEndpoint);
		this.masterRunid = newMasterRunid;
		this.beginOffset += offsetDelta;
		this.endOffset   += offsetDelta;
		
		logger.info("[masterChanged]offsetDelta:{}, masterEndpoint:{}, masterRunid:{}, beginOffset:{}, endOffset:{}",
				offsetDelta,
				this.masterEndpoint, this.masterRunid, this.beginOffset, this.endOffset);
		try {
			saveMeta();
		} catch (IOException e) {
			logger.error("[masterChanged][save meta failed]{}{}{}", newMasterEndpoint, newMasterRunid, offsetDelta);
		}
	}

	@Override
	public void setMasterAddress(Endpoint endpoint) {
		this.masterEndpoint = endpoint;
	}

	@Override
	public Endpoint getMasterAddress() {
		return this.masterEndpoint;
	}

}
