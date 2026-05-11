package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;

/**
 * @author wenchao.meng
 *
 *         Dec 25, 2016
 */
public class LfReader extends AbstractRedisClientProtocol<byte[]> {

	private static final Logger logger = LoggerFactory.getLogger(LfReader.class);

	private static final int INITIAL_BUFFER_SIZE = 64;

	private byte[] buffer;
	private int count;
	private byte[] payload;

	@Override
	public RedisClientProtocol<byte[]> read(ByteBuf byteBuf) {

		int lfIndex = byteBuf.forEachByte(ByteProcessor.FIND_LF);

		if (lfIndex >= 0) {
			int length = lfIndex - byteBuf.readerIndex() + 1;
			if (count == 0) {
				payload = new byte[length];
				byteBuf.readBytes(payload);
			} else {
				System.arraycopy(buffer, 0, payload, 0, count);
				byteBuf.readBytes(payload, count, length);
				count = 0; // 清空缓存
			}
			return this;
		} else {
			int readable = byteBuf.readableBytes();
			if (readable > 0) {
				if (buffer == null) {
					buffer = new byte[Math.max(readable, INITIAL_BUFFER_SIZE)];
				} else {
					ensureCapacity(count + readable);
				}
				byteBuf.readBytes(buffer, count, readable);
				count += readable;
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
		return payload;
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}

	public void reset() {
		count = 0;
		payload = null;
	}

	private void ensureCapacity(int required) {
		if (buffer.length < required) {
			int newSize = Math.max(buffer.length << 1, required);
			buffer = Arrays.copyOf(buffer, newSize);
		}
	}
}
