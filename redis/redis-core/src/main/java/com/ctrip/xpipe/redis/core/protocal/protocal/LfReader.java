package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;

/**
 * @author wenchao.meng
 *
 *         Dec 25, 2016
 */
public class LfReader extends AbstractRedisClientProtocol<byte[]> {

	private ByteArrayOutputStream baous = new ByteArrayOutputStream();

	@Override
	public RedisClientProtocol<byte[]> read(ByteBuf byteBuf) {

		int readable = byteBuf.readableBytes();

		for (int i = 0; i < readable; i++) {

			byte data = byteBuf.readByte();
			if (data == '\n') {
				return this;
			}
			baous.write(data);
		}
		return null;
	}

	@Override
	public boolean supportes(Class<?> clazz) {
		return false;
	}

	@Override
	protected ByteBuf getWriteByteBuf() {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] getPayload() {
		return baous.toByteArray();
	}
}
