package com.ctrip.xpipe.redis.core.protocal.protocal;


import java.io.IOException;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.payload.ByteArrayWritableByteChannel;
import com.ctrip.xpipe.payload.StringInOutPayload;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;


/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午2:35:36
 */
public class BulkStringParser extends AbstractRedisClientProtocol<InOutPayload>{
	
	private Long totalLength = 0L;
	private Long currentLength = 0L;
	private BULK_STRING_STATE  bulkStringState = BULK_STRING_STATE.READING_LENGTH;
	private BulkStringParserListener bulkStringParserListener;
	
	
	public enum BULK_STRING_STATE{
		READING_LENGTH,
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
	
	/**
	 * 返回值暂时没用
	 * @throws IOException 
	 */
	@Override
	public RedisClientProtocol<InOutPayload> read(ByteBuf byteBuf){
		
		
		switch(bulkStringState){
		
			case READING_LENGTH:
				totalLength = readLengthFiled(byteBuf);
				
				if(totalLength == null){
					return null;
				}
				
				if(logger.isDebugEnabled()){
					logger.debug("[parse][length]" + totalLength);
				}
				
				if(bulkStringParserListener != null){
					bulkStringParserListener.onGotLengthFiled(totalLength);
				}
				
				if(totalLength < 0){
					throw new RedisRuntimeException("length < 0:" + totalLength);
				}
				bulkStringState = BULK_STRING_STATE.READING_CONTENT;
				payload.startInput();
			case READING_CONTENT:
				
				int readable = byteBuf.readableBytes();
				int readCount = readable;
				int readerIndex = byteBuf.readerIndex();
				if(currentLength + readable > totalLength){
					readCount = (int) (totalLength - currentLength);
				}
				
				
				int length = 0;
				try {
					length = payload.in(byteBuf.slice(readerIndex, readCount));
				} catch (IOException e) {
					logger.error("[read][exception]" + payload ,e);
					throw new RedisRuntimeException("[write to payload exception]" + payload, e);
				}
				byteBuf.readerIndex(readerIndex + length);
				
				currentLength += length;
				if(currentLength.equals(totalLength)){
					//read finished
					payload.endInput();
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

	
	private Long readLengthFiled(ByteBuf byteBuf){
		
		String lengthStr = readTilCRLFAsString(byteBuf);
		if(lengthStr == null){
			return null;
		}
		lengthStr = lengthStr.trim();
		if(lengthStr.charAt(0) == DOLLAR_BYTE){
			lengthStr = lengthStr.substring(1);
		}
		return Long.parseLong(lengthStr);
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
		
		void onGotLengthFiled(long length);
	}
}
