package com.ctrip.xpipe.simple;

import com.ctrip.xpipe.AbstractTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * Nov 15, 2016
 */
public class NettySimpleTest extends AbstractTest{
	
	@Test
	public void testNettyInternalBuffer() throws IOException{
		
		ByteBufAllocator allocator = new PooledByteBufAllocator(true);
		
		final ByteBuf byteBuf = allocator.buffer(1 << 10);
		byteBuf.writeBytes("1234567890".getBytes());
		
		System.out.println(byteBuf.readableBytes());
		
		scheduled.scheduleAtFixedRate(new Runnable() {
			
			@Override
			public void run() {
//				ByteBuffer byteBuffer = byteBuf.internalNioBuffer(0, byteBuf.readableBytes());
				byteBuf.nioBuffers();
			}
		}, 0, 100, TimeUnit.MICROSECONDS);
		
		System.out.println(byteBuf.readableBytes());
		
		waitForAnyKeyToExit();
	}

}
