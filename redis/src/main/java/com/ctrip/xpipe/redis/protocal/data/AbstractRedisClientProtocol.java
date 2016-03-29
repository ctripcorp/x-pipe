package com.ctrip.xpipe.redis.protocal.data;



import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.protocal.RedisClietProtocol;
import com.ctrip.xpipe.utils.StringUtil;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午6:29:33
 */
public abstract class AbstractRedisClientProtocol<T> extends AbstractRedisProtocol implements RedisClietProtocol<T>{
	
	protected final T payload;
	
	public AbstractRedisClientProtocol() {
		this(null);
	}
	
	public AbstractRedisClientProtocol(T payload) {
		this.payload = payload;
	}
	

	
	@Override
	public void write(OutputStream ous) throws IOException {
		doWrite(ous);
	}
	
	protected abstract void doWrite(OutputStream ous) throws IOException;

	/**
	 * @param byteBuf
	 * @return 结束则返回对应byte[], 否则返回null
	 * @throws IOException 
	 */
	protected byte[] readTilCRLF(InputStream ins) throws IOException {
		
		ByteArrayOutputStream baous = new ByteArrayOutputStream();
		CRLF_STATE state = CRLF_STATE.CONTENT;
		while(true){
			
			int data = ins.read();
			if(data == -1){
				logger.error("[readTilCRLF][EOF]");
				throw new RedisRuntimeException("expected CRLF, but eof found");
			}
			
			baous.write(data);
			
			if(data == '\r'){
				state = CRLF_STATE.CR;
			}else if(data == '\n'){
				if(state == CRLF_STATE.CR){
					state = CRLF_STATE.CRLF;
					break;
				}
			}else{
				state = CRLF_STATE.CONTENT;
			}
		}
		
		return baous.toByteArray();
		
	}

	
	protected  String readTilCRLFAsString(InputStream ins, Charset charset) throws IOException{
		
		byte []bytes = readTilCRLF(ins);
		if(bytes == null){
			return null;
		}
		String ret = new String(bytes, charset);
		if(logger.isInfoEnabled()){
			logger.info("[readTilCRLFAsString]" + ret.trim());
		}
		return ret;
		
	}

	protected  String readTilCRLFAsString(InputStream ins) throws IOException{

		return readTilCRLFAsString(ins, Codec.defaultCharset);
	}


	public enum CRLF_STATE{
		CR,
		CRLF,
		CONTENT
	}

	protected void write(OutputStream ous, byte[] commandBytes) throws IOException {
		if(logger.isInfoEnabled()){
			logger.info("[write]" + new String(commandBytes, Codec.defaultCharset));
		}
		ous.write(commandBytes);
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
}
