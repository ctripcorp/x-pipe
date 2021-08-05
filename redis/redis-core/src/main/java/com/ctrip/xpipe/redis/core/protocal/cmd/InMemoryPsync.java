package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.RdbBulkStringParser;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Aug 26, 2016
 */
public class InMemoryPsync extends AbstractPsync{
	
	private String requestMasterId;
	private Long   requestMasterOffset;
	private ByteArrayOutputStream commands = new ByteArrayOutputStream();
	private ByteArrayOutputStreamPayload rdb = new ByteArrayOutputStreamPayload();

	public InMemoryPsync(String masterHost, int masterPort, String requestMasterId, long   requestMasterOffset, ScheduledExecutorService scheduled) {
		super(masterHost, masterPort, true, scheduled);
		this.requestMasterId = requestMasterId;
		this.requestMasterOffset = requestMasterOffset;
		
	}

	public InMemoryPsync(SimpleObjectPool<NettyClient> clientPool, String requestMasterId, long   requestMasterOffset, ScheduledExecutorService scheduled) {
		super(clientPool, true, scheduled);
		this.requestMasterId = requestMasterId;
		this.requestMasterOffset = requestMasterOffset;
	}

	@Override
	protected Pair<String, Long> getRequestMasterInfo() {
		return new Pair<String, Long>(requestMasterId, requestMasterOffset);
	}

	@Override
	protected void appendCommands(ByteBuf byteBuf) throws IOException {
		
		commands.write(ByteBufUtils.readToBytes(byteBuf));
	}

	@Override
	protected BulkStringParser createRdbReader() {
		return new RdbBulkStringParser(rdb);
	}
	
	@Override
	protected void failReadRdb(Throwable throwable) {
		getLogger().error("[failReadRdb]", throwable);
	}

	public byte[] getCommands() {
		return commands.toByteArray();
	}
	
	public byte[] getRdb() {
		return rdb.getBytes();
	}
}
