package com.ctrip.xpipe.redis.core.protocal.cmd;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.unidal.tuple.Pair;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser;

import io.netty.buffer.ByteBuf;

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

	public InMemoryPsync(String masterHost, int masterPort, String requestMasterId, long   requestMasterOffset) throws Exception {
		super(NettyPoolUtil.createNettyPool(new InetSocketAddress(masterHost, masterPort)), true);
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
		return new BulkStringParser(rdb);
	}

	public byte[] getCommands() {
		return commands.toByteArray();
	}
	
	public byte[] getRdb() {
		return rdb.getBytes();
	}
}
