package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author wenchao.meng
 *
 *         Dec 25, 2016
 */
public class LfReader extends AbstractRedisClientProtocol<byte[]> {

	private static final Logger logger = LoggerFactory.getLogger(LfReader.class);

	private ByteArrayOutputStream baous = new ByteArrayOutputStream(1 << 6);

	@Override
	public RedisClientProtocol<byte[]> read(ByteBuf byteBuf) {

		int newLineOffset = byteBuf.bytesBefore((byte) '\n');

		if (newLineOffset >= 0) {
			try {
				byteBuf.readBytes(baous, newLineOffset+1);
			} catch (IOException e) {
				throw new RedisRuntimeException("[LfReader] read complete",e);
			}
			return this;
		} else {
			int readable = byteBuf.readableBytes();
			if (readable > 0) {
				try {
					byteBuf.readBytes(baous, readable);
				} catch (IOException e) {
					throw new RedisRuntimeException("[LfReader] read half",e);
				}
			}
			return null;
		}
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
