package com.ctrip.xpipe.payload;

import com.ctrip.xpipe.api.codec.Codec;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;

/**
 * @author wenchao.meng
 *
 * 2016年4月26日 下午7:02:09
 */
public class StringInOutPayload extends AbstractInOutPayload{
	
	private String message;
	
	private Charset charset;
	
	public StringInOutPayload(String message){
		
		this(message, Codec.defaultCharset);
	}

	public StringInOutPayload(String message, Charset charset) {
		
		this.message = message;
		this.charset = charset;
		
	}
	

	@Override
	protected int doIn(ByteBuf byteBuf) throws IOException {
		throw new UnsupportedOperationException("unsupported in");
	}

	@Override
	protected long doOut(WritableByteChannel writableByteChannel) throws IOException {
		
		return writableByteChannel.write(ByteBuffer.wrap(message.getBytes(charset)));
	}

	@Override
	protected void doTruncate(int reduceLen) throws IOException {
		throw new UnsupportedOperationException();
	}
}
