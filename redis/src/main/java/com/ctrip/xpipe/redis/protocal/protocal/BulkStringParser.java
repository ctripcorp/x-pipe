package com.ctrip.xpipe.redis.protocal.protocal;

import java.io.IOException;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.redis.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.protocal.RedisClientProtocol;

import io.netty.buffer.ByteBuf;


/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午2:35:36
 */
public class BulkStringParser extends AbstractRedisClientProtocol<InOutPayload>{
	
	private Long totalLength = 0L;
	private Long currentLength = 0L;
	private BULK_STRING_STATE  bulkStringState = BULK_STRING_STATE.READING_LENGTH;
	
	
	public enum BULK_STRING_STATE{
		READING_LENGTH,
		READING_CONTENT,
		READING_CR,
		READING_LF,
		END
	}
	
	public BulkStringParser(InOutPayload bulkStringPayload) {
		super(bulkStringPayload, false, false);
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
				
				if(logger.isInfoEnabled()){
					logger.info("[parse][length]" + totalLength);
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
					return null;
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
	protected byte[] getWriteBytes() {
		throw new UnsupportedOperationException();		
	}
	
}
