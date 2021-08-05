package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.payload.ByteArrayWritableByteChannel;
import com.ctrip.xpipe.payload.StringInOutPayload;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringEofJudger.JudgeResult;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午2:35:36
 */
public abstract class AbstractBulkStringParser extends AbstractRedisClientProtocol<InOutPayload> {

	protected BulkStringEofJudger eofJudger;
	protected BULK_STRING_STATE  bulkStringState = BULK_STRING_STATE.READING_EOF_JUDGER;
	public enum BULK_STRING_STATE{
		READING_EOF_JUDGER,
		READING_CONTENT,
		END
	}

	public static interface BulkStringParserListener{
		void onEofType(EofType eofType);
	}

	private BulkStringParserListener bulkStringParserListener;

	public void setBlukStringParserListener(BulkStringParserListener bulkStringParserListener) {
		this.bulkStringParserListener = bulkStringParserListener;
	}

	public AbstractBulkStringParser(InOutPayload payload) {
		super(payload, true, true);
	}


	private LfReader eofJudgerReader = new LfReader();

	protected boolean readEofJudger(ByteBuf byteBuf) {
		RedisClientProtocol<byte[]> markBytes= eofJudgerReader.read(byteBuf);
		if(markBytes == null){
			return false;
		}
		eofJudger = BulkStringEofJuderManager.create(markBytes.getPayload());
		return eofJudger != null;
	}

	boolean readPayload(ByteBuf byteBuf) {
		int readerIndex = byteBuf.readerIndex();
		BulkStringEofJudger.JudgeResult result = eofJudger.end(byteBuf.slice());
		int length = 0;
		try {
			length = payload.in(byteBuf.slice(readerIndex, result.getReadLen()));
			if (length != result.getReadLen()) {
				throw new IllegalStateException(String.format("expected readLen:%d, but real:%d", result.getReadLen(), length));
			}
		} catch (IOException e) {
			getLogger().error("[read][exception]" + payload, e);
			throw new RedisRuntimeException("[write to payload exception]" + payload, e);
		}
		byteBuf.readerIndex(readerIndex + length);
		return result.isEnd();
	}

	abstract boolean readContent(ByteBuf byteBuf);

	void startInput() {
		payload.startInput();
	}

	void endInput() {
		int truncate = eofJudger.truncate();
		try {
			if (truncate > 0) {
				payload.endInputTruncate(truncate);
			} else {
				payload.endInput();
			}
		} catch (IOException e) {
			throw new RedisRuntimeException("[write to payload truncate exception]" + payload, e);
		}
	}

	abstract RedisClientProtocol<InOutPayload> getResult();

	@Override
	public RedisClientProtocol<InOutPayload> read(ByteBuf byteBuf) {
		while (true) {
			switch (bulkStringState) {
				case READING_EOF_JUDGER:
					if(!readEofJudger(byteBuf)) {return null;}
					bulkStringState = BULK_STRING_STATE.READING_CONTENT;
					if (bulkStringParserListener != null) {
						bulkStringParserListener.onEofType(eofJudger.getEofType());
					}
					startInput();
				case READING_CONTENT:
					if(!readContent(byteBuf)) { return null;}
					endInput();
					bulkStringState = BULK_STRING_STATE.END;
				case END:
					return getResult();
			}
		}
	}



	@Override
	protected ByteBuf getWriteByteBuf() {
		
		if(payload == null){
			if(getLogger().isInfoEnabled()){
				getLogger().info("[getWriteBytes][payload null]");
			}
			return Unpooled.wrappedBuffer(new byte[0]);
		}
		
		if((payload instanceof StringInOutPayload )|| (payload instanceof ByteArrayOutputStreamPayload)){
			try {
				ByteArrayWritableByteChannel channel = new ByteArrayWritableByteChannel();
				payload.out(channel);
				byte []content = channel.getResult();
				String length = String.valueOf((char)DOLLAR_BYTE) + content.length + RedisClientProtocol.CRLF;
				return Unpooled.wrappedBuffer(length.getBytes(), content, RedisClientProtocol.CRLF.getBytes()); 
			} catch (IOException e) {
				getLogger().error("[getWriteBytes]", e);
				return Unpooled.wrappedBuffer(new byte[0]);
			}
		}
		throw new UnsupportedOperationException();		
	}

	@Override
	public boolean supportes(Class<?> clazz) {
		return InOutPayload.class.isAssignableFrom(clazz);
	}
}
