package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午6:29:33
 */
public abstract class AbstractRedisClientProtocol<T> extends AbstractRedisProtocol implements RedisClientProtocol<T>{
	
	protected final T payload;
	
	protected final boolean logRead;

	protected final boolean logWrite;

	private ByteArrayOutputStream baous = new ByteArrayOutputStream();
	private CRLF_STATE 			  crlfState = CRLF_STATE.CONTENT;
	

	public AbstractRedisClientProtocol() {
		this(null, true, true);
	}
	
	public AbstractRedisClientProtocol(T payload, boolean logRead, boolean logWrite) {
		this.payload = payload;
		this.logRead = logRead;
		this.logWrite = logWrite;
	}
	

	
	@Override
	public ByteBuf format(){
		
		ByteBuf byteBuf = getWriteByteBuf();
		
		if(logWrite && getLogger().isDebugEnabled()){
			
			getLogger().info("[getWriteBytes]" + getPayloadAsString());
		}
		return byteBuf;
	}
	
	protected String getPayloadAsString() {
		
		String payloadString = payload.toString();
		if(payload instanceof String[]){
			payloadString = StringUtil.join(" ", (String[])payload); 
		}
		return  payloadString;
	}

	protected abstract ByteBuf getWriteByteBuf();

	/**
	 * @param byteBuf
	 * @return 结束则返回对应byte[], 否则返回null
	 * @throws IOException 
	 */
	protected byte[] readTilCRLF(ByteBuf byteBuf){
		
		switch(crlfState){
			case CONTENT:
			case CR:
				ByteArrayOutputStream result = _readTilCRLF(byteBuf);
				
				if(result == null){
					break;
				}
			case CRLF:
				return baous.toByteArray();
				
		}
		return null;
	}

	
	private ByteArrayOutputStream _readTilCRLF(ByteBuf byteBuf) {
		
		int readable = byteBuf.readableBytes();
		for(int i=0; i < readable ;i++){
			
			byte data = byteBuf.readByte();
			baous.write(data);
			switch(data){
				case '\r':
					crlfState = CRLF_STATE.CR;
					break;
				case '\n':
					if(crlfState == CRLF_STATE.CR){
						crlfState = CRLF_STATE.CRLF;
						break;
					}
				default:
					crlfState = CRLF_STATE.CONTENT;
					break;
			}
			
			if(crlfState == CRLF_STATE.CRLF){
				return baous;
			}
		}
		return null;
		
	}

	protected  String readTilCRLFAsString(ByteBuf byteBuf, Charset charset){
		
		byte []bytes = readTilCRLF(byteBuf);
		if(bytes == null){
			return null;
		}
		String ret = new String(bytes, charset);
		if(getLogger().isDebugEnabled() && logRead){
			getLogger().info("[readTilCRLFAsString]" + ret.trim());
		}
		return ret;
		
	}

	protected  String readTilCRLFAsString(ByteBuf byteBuf){

		return readTilCRLFAsString(byteBuf, Codec.defaultCharset);
	}

	protected byte[] getRequestBytes(Byte sign, Long data) {
		
		StringBuilder sb = new StringBuilder();
		sb.append((char)sign.byteValue());
		sb.append(data);
		sb.append("\r\n");
		return sb.toString().getBytes();
	}

	protected byte[] getRequestBytes(Byte sign, String ... commands) {
		return getRequestBytes(Codec.defaultCharset, sign, commands);
	}

	protected byte[] getRequestBytes(Charset charset, Byte sign, String ... commands) {
		
		StringBuilder sb = new StringBuilder();
		if(sign != null){
			sb.append((char)sign.byteValue());
		}
		sb.append(StringUtil.join(" ",commands));
		sb.append("\r\n");
		return sb.toString().getBytes(charset);
	}


	protected byte[] getRequestBytes(String ... commands) {
		return getRequestBytes(Codec.defaultCharset, commands);
	}

	protected byte[] getRequestBytes(Charset charset, String ... commands) {
		return getRequestBytes(charset, null, commands);
	}

	
	@Override
	public T getPayload() {
		return payload;
	}

	protected abstract Logger getLogger();

	public enum CRLF_STATE{
		CR,
		CRLF,
		CONTENT
	}

	@Override
	public void reset() {
		baous = new ByteArrayOutputStream();
		crlfState = CRLF_STATE.CONTENT;
	}
}
