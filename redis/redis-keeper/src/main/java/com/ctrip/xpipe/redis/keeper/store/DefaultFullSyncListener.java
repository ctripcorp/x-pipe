package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.CommandFile;
import com.ctrip.xpipe.redis.core.store.FullSyncListener;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationProgress;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *
 *         2016年5月9日 下午5:31:00
 */
public class DefaultFullSyncListener implements FullSyncListener {

	private static Logger logger = LoggerFactory.getLogger(DefaultFullSyncListener.class);

	private RedisSlave redisSlave;

	private AtomicLong writtenLength = new AtomicLong();

	public DefaultFullSyncListener(RedisSlave redisSlave) {
		this.redisSlave = redisSlave;
	}

	@Override
	public boolean supportRdb(RdbStore.Type rdbType) {
		return redisSlave.supportRdb(rdbType);
	}

	@Override
	public void onFileData(ReferenceFileRegion referenceFileRegion) {
		
		if (referenceFileRegion == null) {

			if (logger.isInfoEnabled()) {
				logger.info("[rdb write complete]" + redisSlave + "," + writtenLength);
			}
			redisSlave.rdbWriteComplete();
			return;
		}
		writtenLength.addAndGet(referenceFileRegion.count());
		redisSlave.writeFile(referenceFileRegion);
	}

	public void setRdbFileInfo(EofType eofType, ReplicationProgress<?> progress) {
		if (logger.isInfoEnabled()) {
			logger.info("[setRdbFileInfo]eofType:" + eofType + ",progress:" + progress);
		}

		redisSlave.beginWriteRdb(eofType, progress);
	}

	@Override
	public boolean supportProgress(Class<? extends ReplicationProgress<?>> clazz) {
		return redisSlave.supportProgress(clazz);
	}

	@Override
	public boolean isOpen() {
		return redisSlave.isOpen();
	}

	@Override
	public void exception(Exception e) {

		try {
			logger.error("[exception][close client]" + redisSlave, e);
			redisSlave.close();
		} catch (IOException e1) {
			logger.error("[exception]" + redisSlave, e1);
		}
	}

	@Override
	public void beforeFileData() {
	}

	@Override
	public ChannelFuture onCommand(CommandFile currentFile, long filePosition, Object cmd) {
		return redisSlave.onCommand(currentFile, filePosition, cmd);
	}

	@Override
	public void beforeCommand() {
		redisSlave.beforeCommand();
	}

	@Override
	public Long processedOffset() {
		return redisSlave.getAck();
	}

	@Override
	public String toString() {
		return String.format("%s:%s", getClass().getSimpleName(), redisSlave);
	}
}
