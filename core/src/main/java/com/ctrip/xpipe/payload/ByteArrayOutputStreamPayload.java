package com.ctrip.xpipe.payload;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * 2016年4月24日 下午8:53:30
 */
public class ByteArrayOutputStreamPayload extends AbstractInOutPayload{
	
	private ByteArrayOutputStream baous;

	@Override
	public void doStartInput() {
		 baous = new ByteArrayOutputStream();
	}

	@Override
	public int doIn(ByteBuf byteBuf) throws IOException {
		int size = byteBuf.readableBytes();
		byteBuf.readBytes(baous, size);
		
		return size - byteBuf.readableBytes();
	}

	@Override
	public long doOut(WritableByteChannel writableByteChannel) throws IOException {
		
		byte[]result = baous.toByteArray();
		return writableByteChannel.write(ByteBuffer.wrap(result));
	}

	
	public byte[] getBytes(){
		return baous.toByteArray();
	}
}
