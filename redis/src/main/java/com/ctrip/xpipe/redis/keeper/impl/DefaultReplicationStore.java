package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.helper.Files.IO;

import com.alibaba.fastjson.JSON;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.RdbFileListener;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.impl.RdbStore.Status;

import io.netty.buffer.ByteBuf;

public class DefaultReplicationStore implements ReplicationStore {

	private final static Logger log = LoggerFactory.getLogger(DefaultReplicationStore.class);

	private static final String META_FILE = "meta.json";

	private static final String ROOT_FILE_PATTERN = "root-%s.json";

	private File baseDir;

	private RdbStore rdbStore;

	private RandomAccessFile rdbReadFile;

	private CommandStore cmdStore;

	private ConcurrentMap<CommandsListener, CommandNotifier> cmdListeners = new ConcurrentHashMap<>();

	private AtomicLong endOffset = new AtomicLong();

	private int cmdFileSize;

	private AtomicReference<ReplicationStoreMeta> metaRef = new AtomicReference<>();

	public DefaultReplicationStore(File baseDir, int cmdFileSize) throws IOException {
		this.baseDir = baseDir;
		this.cmdFileSize = cmdFileSize;
		loadMeta();

		ReplicationStoreMeta meta = metaRef.get();

		if (meta.getRdbFile() != null) {
			File rdb = new File(baseDir, meta.getRdbFile());
			if (rdb.isFile()) {
				rdbStore = new DefaultRdbStore(rdbFileOf(meta.getRdbFile()), meta.getRdbFileSize());
				rdbReadFile = rdbStore.getRdbFile();
				cmdStore = new DefaultCommandStore(cmdFileOf(meta.getRdbFile()), cmdFileSize);
			}
		}
	}

	@Override
	public void close() throws IOException {
		// TODO
	}

	@Override
	public void beginRdb(String masterRunid, long masterOffset, long rdbFileSize) throws IOException {
		log.info("Begin RDB masterRunid:{}, masterOffset:{}, rdbFileSize:{}", masterRunid, masterOffset, rdbFileSize);
		ReplicationStoreMeta meta = metaRef.get();

		meta.setMasterRunid(masterRunid);
		meta.setBeginOffset(masterOffset + 1);
		meta.setRdbFile(UUID.randomUUID().toString());
		meta.setRdbFileSize(rdbFileSize);
		this.endOffset.set(masterOffset);

		baseDir.mkdirs();
		saveMeta();

		// TODO file naming
		rdbStore = new DefaultRdbStore(rdbFileOf(meta.getRdbFile()), rdbFileSize);
		rdbReadFile = rdbStore.getRdbFile();
		cmdStore = new DefaultCommandStore(cmdFileOf(meta.getRdbFile()), cmdFileSize);
	}

	private File cmdFileOf(String masterRunid) {
		return new File(baseDir, masterRunid + "_cmd");
	}

	private File rdbFileOf(String masterRunid) {
		return new File(baseDir, masterRunid);
	}

	private void saveMeta() throws IOException {
		// TODO file naming
		IO.INSTANCE.writeTo(new File(baseDir, META_FILE), JSON.toJSONString(metaRef.get()));
	}

	private void loadMeta() throws IOException {
		File metaFile = new File(baseDir, META_FILE);
		if (metaFile.isFile()) {
			metaRef.set(JSON.parseObject(IO.INSTANCE.readFrom(metaFile, "utf-8"), ReplicationStoreMeta.class));
			ReplicationStoreMeta meta = metaRef.get();

			File rdbFile = rdbFileOf(meta.getMasterRunid());
			if (rdbFile.isFile()) {
				endOffset.set(meta.getBeginOffset() + rdbFile.length() - 1);
			} else {
				endOffset.set(meta.getBeginOffset() - 1);
			}

			log.info("Meta loaded: {}, endOffset: {}", meta, endOffset);
		} else {
			metaRef.set(new ReplicationStoreMeta());
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
		return metaRef.get().getMasterRunid();
	}

	@Override
	public int appendCommands(ByteBuf byteBuf) throws IOException {
		int wrote = cmdStore.appendCommands(byteBuf);
		endOffset.addAndGet(wrote);

		return wrote;
	}

	@Override
	public void addCommandsListener(long offset, CommandsListener commandsListener) throws IOException {
		CommandNotifier notifier = new DefaultCommandNotifier();
		notifier.start(cmdStore, offset - metaRef.get().getBeginOffset(), commandsListener);
		cmdListeners.put(commandsListener, notifier);
	}

	@Override
	public void removeCommandsListener(CommandsListener commandsListener) {
		cmdListeners.remove(commandsListener);
	}

	@Override
	public long beginOffset() {
		return metaRef.get().getBeginOffset();
	}

	@Override
	public long endOffset() {
		return endOffset.get();
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
					log.error("Error read rdb file", e);
				}
			}
		}.start();
	}

	private void doReadRdbFile(RdbFileListener rdbFileListener) throws IOException {
		// beginOffset - 1 == masteroffset
		rdbFileListener.setRdbFileInfo(metaRef.get().getRdbFileSize(), metaRef.get().getKeeperBeginOffset() - 1);
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
	public void masterChanged(DefaultEndPoint newMasterEndpoint, String newMasterRunid, long offsetDelta) {
		ReplicationStoreMeta newMeta = new ReplicationStoreMeta(metaRef.get());

		newMeta.setMasterAddress(newMasterEndpoint);
		newMeta.setMasterRunid(newMasterRunid);
		newMeta.setBeginOffset(metaRef.get().getBeginOffset() + offsetDelta);

		metaRef.set(newMeta);
		this.endOffset.addAndGet(offsetDelta);

		log.info("[masterChanged]offsetDelta:{}, masterEndpoint:{}, masterRunid:{}, beginOffset:{}, endOffset:{}", offsetDelta, newMeta.getMasterAddress(),
				newMeta.getMasterRunid(), newMeta.getBeginOffset(), endOffset.get());
		try {
			saveMeta();
		} catch (IOException e) {
			log.error("[masterChanged][save meta failed]{}{}{}", newMasterEndpoint, newMasterRunid, offsetDelta);
		}
	}

	@Override
	public void setMasterAddress(DefaultEndPoint endpoint) {
		metaRef.get().setMasterAddress(endpoint);
	}

	@Override
	public DefaultEndPoint getMasterAddress() {
		return metaRef.get().getMasterAddress();
	}

	@Override
	public void setKeeperBeginOffset(long keeperBeginOffset) {

		metaRef.get().setKeeperBeginOffset(keeperBeginOffset);
	}

	@Override
	public long getKeeperBeginOffset() {

		return metaRef.get().getKeeperBeginOffset();
	}

	@Override
	public void setActive(boolean active) {
		metaRef.get().setActive(active);
	}

	@Override
	public boolean isActive() {
		return metaRef.get().isActive();
	}

	@Override
	public ReplicationStoreMeta getReplicationStoreMeta() {
		return metaRef.get();
	}

	@Override
	public ReplicationStoreMeta getReplicationStoreMeta(String name) throws IOException {
		File file = new File(baseDir, String.format(ROOT_FILE_PATTERN, name));

		ReplicationStoreMeta meta = null;
		if (file.isFile()) {
			meta = JSON.parseObject(IO.INSTANCE.readFrom(file, "utf-8"), ReplicationStoreMeta.class);
		}

		return meta;
	}

	@Override
	public void saveMeta(String name, ReplicationStoreMeta replicationStoreMeta) throws IOException {
		File file = new File(baseDir, String.format(ROOT_FILE_PATTERN, name));

		IO.INSTANCE.writeTo(file, JSON.toJSONString(replicationStoreMeta));
	}

	@Override
	public void changeMetaTo(String name) throws IOException {
		ReplicationStoreMeta meta = getReplicationStoreMeta(name);

		
		if (meta != null) {
			ReplicationStoreMeta old = getReplicationStoreMeta();
			long length = endOffset() - beginOffset();
			long newEndOffset = meta.getBeginOffset() + length;

			metaRef.set(meta);
			endOffset.set(newEndOffset);
			log.info("[changeMetaTo][cmdFile length, new endoffset]{},{}", length, newEndOffset);
			log.info("[changeMetaTo]{}", old);
			log.info("[changeMetaTo]{}", meta);
		} else {
			throw new IllegalStateException("can not find meta:" + name);
		}
	}

	@Override
	public boolean isFresh() {
		return metaRef.get().getMasterRunid() == null;
	}
}
