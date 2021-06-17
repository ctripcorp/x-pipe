package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser.BulkStringParserListener;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author marsqing
 *
 *         2016年3月24日 下午2:24:38
 */
public abstract class AbstractPsync extends AbstractRedisCommand<Object> implements Psync, BulkStringParserListener {

	private boolean saveCommands;

	private BulkStringParser rdbReader;
	
	private String replIdRequest;
	private long offsetRequest;

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
	protected void doExecute() throws CommandExecutionException {
		super.doExecute();
		addFutureListener();
		
	}

	//public for unit test
	public void addFutureListener() {
		future().addListener(new CommandFutureListener<Object>() {
			@Override
			public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
				if(!commandFuture.isSuccess()){
					failPsync(commandFuture.cause());
				}
			}
		});
	}

	protected void failPsync(Throwable throwable) {
		if(psyncState == PSYNC_STATE.READING_RDB){
			failReadRdb(throwable);
		}
	}

	protected abstract void failReadRdb(Throwable throwable);

	@Override
	public ByteBuf getRequest() {

		Pair<String, Long> requestInfo = getRequestMasterInfo();

		replIdRequest = requestInfo.getKey();
		offsetRequest = requestInfo.getValue();

		if (replIdRequest == null) {
			replIdRequest = "?";
			offsetRequest = -1;
		}
		RequestStringParser requestString = new RequestStringParser(getName(), replIdRequest,
				String.valueOf(offsetRequest));
		if (getLogger().isDebugEnabled()) {
			getLogger().debug("[doRequest]{}, {}", this, StringUtil.join(" ", requestString.getPayload()));
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
		while(true) {
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
						getLogger().info("[doReceiveResponse][createRdbReader]{}", ChannelUtil.getDesc(channel));
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
						break;
					} else {
						break;
					}
				case READING_COMMANDS:
					if (saveCommands) {
						try {
							appendCommands(byteBuf);
						} catch (IOException e) {
							getLogger().error("[doHandleResponse][write commands error]" + this, e);
						}
					}
					break;
				default:
					throw new IllegalStateException("unknown state:" + psyncState);
			}

			return null;
		}
	}

	protected void handleRedisResponse(Channel channel, String psync) throws IOException {

		if (getLogger().isInfoEnabled()) {
			getLogger().info("[handleRedisResponse]{}, {}, {}", ChannelUtil.getDesc(channel), this, psync);
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
			getLogger().debug("[readRedisResponse]{}, {}, {}, {}", ChannelUtil.getDesc(channel), this, replId,
					masterRdbOffset);
			psyncState = PSYNC_STATE.READING_RDB;

			doOnFullSync();
		} else if (split[0].equalsIgnoreCase(PARTIAL_SYNC)) {

			psyncState = PSYNC_STATE.READING_COMMANDS;
			
			String newReplId = null;
			if(split.length >= 2 && split[1].length() == RedisProtocol.RUN_ID_LENGTH){
				newReplId = split[1];
			}
			doOnContinue(newReplId);
		} else {
			throw new RedisRuntimeException("unknown reply:" + psync);
		}
	}

	protected void endReadRdb() {

		getLogger().info("[endReadRdb]");
		for (PsyncObserver observer : observers) {
			try {
				observer.endWriteRdb();
			} catch (Throwable th) {
				getLogger().error("[endReadRdb]" + this, th);
			}
		}
	}

	protected abstract void appendCommands(ByteBuf byteBuf) throws IOException;

	protected abstract BulkStringParser createRdbReader();

	protected void doOnFullSync() throws IOException {
		getLogger().debug("[doOnFullSync]");
		notifyFullSync();
	}

	private void notifyFullSync() {
		getLogger().debug("[notifyFullSync]");
		for (PsyncObserver observer : observers) {
			observer.onFullSync(masterRdbOffset);
		}
	}
	
	protected void doOnContinue(String newReplId) throws IOException{
		getLogger().debug("[doOnContinue]{}",newReplId);
		notifyContinue(newReplId);
	}

	private void notifyContinue(String newReplId) {

		for (PsyncObserver observer : observers) {
			observer.onContinue(replIdRequest, newReplId);
		}
	}

	@Override
	public void onEofType(EofType eofType) {
		beginReadRdb(eofType);
	}

	protected void beginReadRdb(EofType eofType) {

		getLogger().info("[beginReadRdb]{}, eof:{}", this, eofType);

		for (PsyncObserver observer : observers) {
			try {
				observer.beginWriteRdb(eofType, replId, masterRdbOffset);
			} catch (Throwable th) {
				getLogger().error("[beginReadRdb]" + this, th);
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
