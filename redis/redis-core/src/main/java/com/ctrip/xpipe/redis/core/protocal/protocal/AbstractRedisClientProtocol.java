package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午6:29:33
 */
public abstract class AbstractRedisClientProtocol<T> extends AbstractRedisProtocol implements RedisClientProtocol<T>{
	
	protected final T payload;
	
	protected final boolean logRead;

	protected final boolean logWrite;

	private static final int DEFAULT_LINE_BUFFER_SIZE = 1 << 6;

	private byte[] lineBuffer = new byte[DEFAULT_LINE_BUFFER_SIZE];
	private int lineBufferLen = 0;
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
				if(!_readTilCRLF(byteBuf)){
					break;
				}
			case CRLF:
				return consumeLineBuffer();
		}
		return null;
	}

	private byte[] consumeLineBuffer() {
		byte[] line = Arrays.copyOf(lineBuffer, lineBufferLen);
		lineBufferLen = 0;
		crlfState = CRLF_STATE.CONTENT;
		return line;
	}

	private boolean _readTilCRLF(ByteBuf byteBuf) {

		while (byteBuf.isReadable()) {
			int readerIndex = byteBuf.readerIndex();
			int readable = byteBuf.readableBytes();
			int lfIndex = byteBuf.forEachByte(readerIndex, readable, ByteProcessor.FIND_LF);

			if (lfIndex < 0) {
				appendFromByteBuf(byteBuf, readable);
				updateStateAfterRead(readable);
				return false;
			}

			int readLen = lfIndex - readerIndex + 1;
			appendFromByteBuf(byteBuf, readLen);
			updateStateAfterRead(readLen);
			if (crlfState == CRLF_STATE.CRLF) {
				return true;
			}
		}
		return false;
	}

	private void appendFromByteBuf(ByteBuf byteBuf, int len) {
		if (len <= 0) {
			return;
		}
		ensureLineBufferCapacity(lineBufferLen + len);
		byteBuf.readBytes(lineBuffer, lineBufferLen, len);
		lineBufferLen += len;
	}

	private void ensureLineBufferCapacity(int needLength) {
		if (needLength <= lineBuffer.length) {
			return;
		}
		int newSize = lineBuffer.length;
		while (newSize < needLength) {
			newSize <<= 1;
		}
		lineBuffer = Arrays.copyOf(lineBuffer, newSize);
	}

	private void updateStateAfterRead(int readLen) {
		if (readLen <= 0) {
			return;
		}
		int lastIndex = lineBufferLen - 1;
		byte last = lineBuffer[lastIndex];
		if (last == '\r') {
			crlfState = CRLF_STATE.CR;
			return;
		}
		if (last == '\n') {
			if (lastIndex > 0 && lineBuffer[lastIndex - 1] == '\r') {
				crlfState = CRLF_STATE.CRLF;
				return;
			}
			crlfState = CRLF_STATE.CONTENT;
			return;
		}
		crlfState = CRLF_STATE.CONTENT;
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
		lineBufferLen = 0;
		crlfState = CRLF_STATE.CONTENT;
	}
}
