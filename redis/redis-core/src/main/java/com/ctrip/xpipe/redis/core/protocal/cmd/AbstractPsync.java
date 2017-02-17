package com.ctrip.xpipe.redis.core.protocal.cmd;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.unidal.tuple.Pair;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser.BulkStringParserListener;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.StringUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author marsqing
 *
 *         2016年3月24日 下午2:24:38
 */
public abstract class AbstractPsync extends AbstractRedisCommand<Object> implements Psync, BulkStringParserListener {

	private boolean saveCommands;

	private BulkStringParser rdbReader;

	protected String replId;

	protected long masterRdbOffset;

	protected List<PsyncObserver> observers = new LinkedList<PsyncObserver>();

	protected PSYNC_STATE psyncState = PSYNC_STATE.PSYNC_COMMAND_WAITING_REPONSE;

	public AbstractPsync(String host, int port, boolean saveCommands, ScheduledExecutorService scheduled) {
		super(host, port, scheduled);
		this.saveCommands = saveCommands;
	}

	public AbstractPsync(SimpleObjectPool<NettyClient> clientPool, boolean saveCommands,
			ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
		this.saveCommands = saveCommands;
	}

	@Override
	public String getName() {
		return "psync";
	}

	@Override
	public ByteBuf getRequest() {

		Pair<String, Long> requestInfo = getRequestMasterInfo();

		String replIdRequest = requestInfo.getKey();
		long offset = requestInfo.getValue();

		if (replIdRequest == null) {
			replIdRequest = "?";
			offset = -1;
		}
		RequestStringParser requestString = new RequestStringParser(getName(), replIdRequest,
				String.valueOf(offset));
		if (logger.isDebugEnabled()) {
			logger.debug("[doRequest]{}, {}", this, StringUtil.join(" ", requestString.getPayload()));
		}
		return requestString.format();
	}

	protected abstract Pair<String, Long> getRequestMasterInfo();

	@Override
	public void addPsyncObserver(PsyncObserver observer) {
		this.observers.add(observer);
	}

	@Override
	protected Object doReceiveResponse(Channel channel, ByteBuf byteBuf) throws Exception {

		switch (psyncState) {

		case PSYNC_COMMAND_WAITING_REPONSE:
			Object response = super.doReceiveResponse(channel, byteBuf);
			if (response == null) {
				return null;
			}
			handleRedisResponse(channel, (String) response);
			break;

		case READING_RDB:

			if (rdbReader == null) {
				logger.info("[doReceiveResponse][createRdbReader]{}", ChannelUtil.getDesc(channel));
				rdbReader = createRdbReader();
				rdbReader.setBulkStringParserListener(this);
			}

			RedisClientProtocol<InOutPayload> payload = rdbReader.read(byteBuf);
			if (payload != null) {
				psyncState = PSYNC_STATE.READING_COMMANDS;
				if (!saveCommands) {
					future().setSuccess();
				}
				endReadRdb();
			} else {
				break;
			}
		case READING_COMMANDS:
			if (saveCommands) {
				try {
					appendCommands(byteBuf);
				} catch (IOException e) {
					logger.error("[doHandleResponse][write commands error]" + this, e);
				}
			}
			break;
		default:
			throw new IllegalStateException("unknown state:" + psyncState);
		}

		return null;
	}

	protected void handleRedisResponse(Channel channel, String psync) throws IOException {

		if (logger.isInfoEnabled()) {
			logger.info("[handleRedisResponse]{}, {}, {}", ChannelUtil.getDesc(channel), this, psync);
		}
		String[] split = splitSpace(psync);
		if (split.length == 0) {
			throw new RedisRuntimeException("wrong reply:" + psync);
		}

		if (split[0].equalsIgnoreCase(FULL_SYNC)) {
			if (split.length != 3) {
				throw new RedisRuntimeException("unknown reply:" + psync);
			}
			replId = split[1];
			masterRdbOffset = Long.parseLong(split[2]);
			logger.debug("[readRedisResponse]{}, {}, {}, {}", ChannelUtil.getDesc(channel), this, replId,
					masterRdbOffset);
			psyncState = PSYNC_STATE.READING_RDB;

			doOnFullSync();
		} else if (split[0].equalsIgnoreCase(PARTIAL_SYNC)) {

			psyncState = PSYNC_STATE.READING_COMMANDS;
			notifyContinue();
		} else {
			throw new RedisRuntimeException("unknown reply:" + psync);
		}
	}

	protected void endReadRdb() {

		logger.info("[endReadRdb]");
		for (PsyncObserver observer : observers) {
			try {
				observer.endWriteRdb();
			} catch (Throwable th) {
				logger.error("[endReadRdb]" + this, th);
			}
		}
	}

	protected abstract void appendCommands(ByteBuf byteBuf) throws IOException;

	protected abstract BulkStringParser createRdbReader();

	protected void doOnFullSync() throws IOException {
		logger.debug("[doOnFullSync]");
		notifyFullSync();
	}

	private void notifyFullSync() {
		logger.debug("[notifyFullSync]");
		for (PsyncObserver observer : observers) {
			observer.onFullSync();
		}
	}

	private void notifyContinue() {

		for (PsyncObserver observer : observers) {
			observer.onContinue();
		}
	}

	@Override
	public void onEofType(EofType eofType) {
		beginReadRdb(eofType);
	}

	protected void beginReadRdb(EofType eofType) {

		logger.info("[beginReadRdb]{}, eof:{}", this, eofType);

		for (PsyncObserver observer : observers) {
			try {
				observer.beginWriteRdb(eofType, masterRdbOffset);
			} catch (Throwable th) {
				logger.error("[beginReadRdb]" + this, th);
			}
		}
	}

	protected void notifyReFullSync() {

		for (PsyncObserver observer : observers) {
			observer.reFullSync();
		}
	}

	@Override
	protected Object format(Object payload) {
		return payload;
	}

	@Override
	public int getCommandTimeoutMilli() {
		return 0;
	}

	@Override
	protected void doReset() {
		throw new UnsupportedOperationException("not supported");
	}
}
