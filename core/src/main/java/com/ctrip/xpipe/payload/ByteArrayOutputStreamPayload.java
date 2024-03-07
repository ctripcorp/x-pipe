package com.ctrip.xpipe.payload;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *
 * 2016年4月24日 下午8:53:30
 */
public class ByteArrayOutputStreamPayload extends AbstractInOutPayload{

	private int INIT_SIZE = 2 << 10;
	private byte []data;
	private AtomicInteger pos  = new AtomicInteger(0);
	
	public ByteArrayOutputStreamPayload() {
		
	}

	public ByteArrayOutputStreamPayload(int INIT_SIZE) {
		if(INIT_SIZE > 0) {
			this.INIT_SIZE = INIT_SIZE;
		}
	}

	public ByteArrayOutputStreamPayload(String message) {
		data = message.getBytes();
		pos.set(data.length);
	}

	@Override
	public void doStartInput() {
		 data = new byte[INIT_SIZE];
		 pos.set(0);
	}

	@Override
	public int doIn(ByteBuf byteBuf) throws IOException {
		
		int size = byteBuf.readableBytes();
		
		makeSureSize(size);
		
		byteBuf.readBytes(data, pos.get(), size);
		
		int read = size - byteBuf.readableBytes(); 
		pos.addAndGet(read);
		
		return read;
	}

	private void makeSureSize(int size) {
		
		if(pos.get() + size > data.length){
			byte []newData = new byte[ensureSize(size)];
			System.arraycopy(data, 0, newData, 0, data.length);
			data = newData;
		}
	}

	private int ensureSize(int size) {
		int predictSize = data.length * 2, requireSize = size + pos.get();
		return predictSize > requireSize ? predictSize : requireSize;
	}

	@Override
	public long doOut(WritableByteChannel writableByteChannel) throws IOException {
		
		return writableByteChannel.write(ByteBuffer.wrap(data, 0, pos.get()));
	}

	
	public byte[] getBytes(){
		int currentPos = pos.get();
		
		byte []dst = new byte[currentPos];
		System.arraycopy(data, 0, dst, 0, currentPos);
		return dst;
	}
	
	@Override
	public String toString() {
		
		if(data != null){
			return new String(data, 0 , pos.get());
		}
		return super.toString();
	}

	@Override
	protected void doTruncate(int reduceLen) {
		pos.addAndGet(-reduceLen);
	}
}
