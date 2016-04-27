package com.ctrip.xpipe.payload;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * @author wenchao.meng
 *
 * 2016年4月24日 下午9:44:43
 */
public class ByteArrayWritableByteChannel implements WritableByteChannel{
	
	private ByteArrayOutputStream baous = new ByteArrayOutputStream();
	
	@Override
	public boolean isOpen() {
		return true;
	}

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		
		int offset = src.arrayOffset();
		int length = src.remaining();
		int position = src.position();
		int limit = src.limit();
		baous.write(src.array(), offset + position, offset + limit);
		src.position(limit);
		return length;
	}
	
	public byte []getResult(){
		return baous.toByteArray();
	}

}
