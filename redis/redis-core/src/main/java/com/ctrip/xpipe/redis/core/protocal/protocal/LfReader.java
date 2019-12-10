package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

/**
 * @author wenchao.meng
 *
 *         Dec 25, 2016
 */
public class LfReader extends AbstractRedisClientProtocol<byte[]> {

	private static final Logger logger = LoggerFactory.getLogger(LfReader.class);

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

	@Override
	protected Logger getLogger() {
		return logger;
	}
}
