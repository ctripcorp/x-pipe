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

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午2:35:36
 */
public class BulkStringParser extends AbstractRedisClientProtocol<InOutPayload>{
	
	private BulkStringEofJudger eofJudger;
	private BULK_STRING_STATE  bulkStringState = BULK_STRING_STATE.READING_EOF_MARK;
	private BulkStringParserListener bulkStringParserListener;
	
	
	public enum BULK_STRING_STATE{
		READING_EOF_MARK,
		READING_CONTENT,
		READING_CR,
		READING_LF,
		END
	}
		
	public BulkStringParser(String content){
		this(new StringInOutPayload(content), null);
		
	}
	
	public BulkStringParser(InOutPayload bulkStringPayload) {
		this(bulkStringPayload, null);
	}
	
	public BulkStringParser(InOutPayload bulkStringPayload, BulkStringParserListener bulkStringParserListener) {
		super(bulkStringPayload, false, false);
		this.bulkStringParserListener = bulkStringParserListener;
	}

	public void setBulkStringParserListener(BulkStringParserListener bulkStringParserListener) {
		this.bulkStringParserListener = bulkStringParserListener;
	}
	
	@Override
	public RedisClientProtocol<InOutPayload> read(ByteBuf byteBuf){
		
		switch(bulkStringState){
		
			case READING_EOF_MARK:
				eofJudger = readEOfMark(byteBuf);
				if(eofJudger == null){
					return null;
				}
				
				logger.debug("[read]{}", eofJudger);
				if(bulkStringParserListener != null){
					bulkStringParserListener.onEofType(eofJudger.getEofType());
				}
				
				bulkStringState = BULK_STRING_STATE.READING_CONTENT;
				payload.startInput();
			case READING_CONTENT:
				
				int readerIndex = byteBuf.readerIndex();
				JudgeResult result = eofJudger.end(byteBuf.slice());
				int length = 0;
				try {
					length = payload.in(byteBuf.slice(readerIndex, result.getReadLen()));
					if(length != result.getReadLen()){
						throw new IllegalStateException(String.format("expected readLen:%d, but real:%d", result.getReadLen(), length));
					}
				} catch (IOException e) {
					logger.error("[read][exception]" + payload ,e);
					throw new RedisRuntimeException("[write to payload exception]" + payload, e);
				}
				byteBuf.readerIndex(readerIndex + length);
				
				if(result.isEnd()){
					int truncate = eofJudger.truncate();
					try {
						if(truncate > 0){
								payload.endInputTruncate(truncate);
						}else{
							payload.endInput();
						}
					} catch (IOException e) {
						throw new RedisRuntimeException("[write to payload truncate exception]" + payload, e);
					}
					bulkStringState = BULK_STRING_STATE.READING_CR;
				}else{
					break;
				}
			case READING_CR:
				if(byteBuf.readableBytes() == 0){
					return new BulkStringParser(payload);
				}
				byte data1 = byteBuf.getByte(byteBuf.readerIndex());
				if(data1 == '\r'){
					byteBuf.readByte();
					bulkStringState = BULK_STRING_STATE.READING_LF;
				}else{
					return new BulkStringParser(payload);
				}
			case READING_LF:
				if(byteBuf.readableBytes() == 0){
					return null;
				}
				data1 = byteBuf.getByte(byteBuf.readerIndex());
				if(data1 == '\n'){
					byteBuf.readByte();
					bulkStringState = BULK_STRING_STATE.END;
				}
				return new BulkStringParser(payload);
			case END:
				return new BulkStringParser(payload);
			default:
				break;
		}
		return null;
	}

	private LfReader lfReader = null;

	private BulkStringEofJudger readEOfMark(ByteBuf byteBuf){
		
		if(lfReader == null){
			lfReader = new LfReader();
		}
		RedisClientProtocol<byte[]> markBytes= lfReader.read(byteBuf);
		if(markBytes == null){
			return null;
		}
		
		if(markBytes.getPayload().length == 0){
			lfReader = null;
			return null;
		}
		
		return BulkStringEofJuderManager.create(markBytes.getPayload());
	}

	
	@Override
	protected ByteBuf getWriteByteBuf() {
		
		if(payload == null){
			if(logger.isInfoEnabled()){
				logger.info("[getWriteBytes][payload null]");
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
				logger.error("[getWriteBytes]", e);
				return Unpooled.wrappedBuffer(new byte[0]);
			}
		}
		throw new UnsupportedOperationException();		
	}

	public static interface BulkStringParserListener{
		void onEofType(EofType eofType);
	}

	@Override
	public boolean supportes(Class<?> clazz) {
		return InOutPayload.class.isAssignableFrom(clazz);
	}
}
