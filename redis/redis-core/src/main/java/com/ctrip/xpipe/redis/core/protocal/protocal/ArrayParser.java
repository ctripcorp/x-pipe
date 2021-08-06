package com.ctrip.xpipe.redis.core.protocal.protocal;


import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.payload.InOutPayloadFactory;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 下午6:05:05
 */
public class ArrayParser extends AbstractRedisClientProtocol<Object[]>{

	private static final Logger logger = LoggerFactory.getLogger(ArrayParser.class);
	
	public enum ARRAY_STATE{
		READ_SIZE,
		READ_CONTENT
	}

	private Object[] resultArray;
	private int  arraySize = 0;
	private int  currentIndex = 0;
	private RedisClientProtocol<?> currentParser  = null;
	private ARRAY_STATE arrayState = ARRAY_STATE.READ_SIZE;

	private InOutPayloadFactory inOutPayloadFactory;

	public ArrayParser() {

	}
	
	public ArrayParser(Object []payload){
		super(payload, true, true);
	}

	@Override
	public RedisClientProtocol<Object[]> read(ByteBuf byteBuf) {
		
		
		switch(arrayState){
		
			case READ_SIZE:
				
				String arrayNumString = readTilCRLFAsString(byteBuf);
				if(arrayNumString == null){
					return null;
				}
				
				if(arrayNumString.charAt(0) == ASTERISK_BYTE){
					arrayNumString = arrayNumString.substring(1);
				}
				arrayNumString = arrayNumString.trim();
				
				arraySize = Integer.valueOf(arrayNumString);
				resultArray = new Object[arraySize];
				arrayState = ARRAY_STATE.READ_CONTENT;
				currentIndex = 0;
				if(arraySize == 0){
					return new ArrayParser(resultArray);
				}
				if(arraySize < 0){
					return new ArrayParser(null);
				}
			case READ_CONTENT:
				
				for(int i=currentIndex; i < arraySize ; i++){
					
					while(true){
						if(currentParser == null){
							
							if(byteBuf.readableBytes() == 0){
								return null;
							}
							int readerIndex = byteBuf.readerIndex();
							int data = byteBuf.getByte(readerIndex);
							switch(data){
								case '\r':
								case '\n':
									byteBuf.readByte();
									break;
								case DOLLAR_BYTE:
									if(inOutPayloadFactory != null) {
										currentParser = new CommandBulkStringParser(inOutPayloadFactory.create());
									} else {
										currentParser = new CommandBulkStringParser(new ByteArrayOutputStreamPayload());
									}
									break;
								case COLON_BYTE:
									currentParser = new LongParser();
									break;
								case ASTERISK_BYTE:
									currentParser = new ArrayParser().setInOutPayloadFactory(inOutPayloadFactory);
									break;
								case MINUS_BYTE:
									currentParser = new RedisErrorParser();
									break;
								case PLUS_BYTE:
									currentParser = new SimpleStringParser();
									break;
								default:
									throw new RedisRuntimeException("unknown protocol type:" + (char)data);
							}
						}
						if(currentParser != null){
							break;
						}
					}
					RedisClientProtocol<?> result = currentParser.read(byteBuf);
					if(result == null){
						return null;
					}
					resultArray[currentIndex] = result.getPayload();
					currentParser = null;
					currentIndex++;
				}
				if(currentIndex == arraySize){
					return new ArrayParser(resultArray);
				}
				break;
			default:
				throw new IllegalStateException("unknown state:" + arrayState);
		}
		return null;
	}

	@Override
	protected ByteBuf getWriteByteBuf() {
		
		int length = payload.length;
		CompositeByteBuf result = new CompositeByteBuf(UnpooledByteBufAllocator.DEFAULT, false, payload.length + 1);
		String prefix = String.format("%c%d\r\n", ASTERISK_BYTE, length);
		result.addComponent(Unpooled.wrappedBuffer(prefix.getBytes()));
		for(Object o : payload){
			ByteBuf buff = ParserManager.parse(o);
			result.addComponent(buff);
		}
		result.setIndex(0, result.capacity());
		return result;
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}

	@Override
	public boolean supportes(Class<?> clazz) {
		return clazz.isArray();
	}

	public ArrayParser setInOutPayloadFactory(InOutPayloadFactory inOutPayloadFactory) {
		this.inOutPayloadFactory = inOutPayloadFactory;
		return this;
	}
}
