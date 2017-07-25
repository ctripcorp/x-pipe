package com.ctrip.xpipe.payload;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.testutils.MemoryPrinter;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * 2016年4月24日 下午8:58:12
 */
public class ByteArrayOutputStreamPayloadTest extends AbstractTest{

	@Test
	public void testInout() throws Exception {
		
		ByteArrayOutputStreamPayload payload = new ByteArrayOutputStreamPayload();
		String randomStr = randomString();
		
		ByteBuf byteBuf = directByteBuf(randomStr.length());

		byteBuf.writeBytes(randomStr.getBytes());
		payload.startInput();
		payload.in(byteBuf);
		payload.endInput();
		
		
		final ByteBuf result = directByteBuf(randomStr.length());

		payload.startOutput();
		long wroteLength = payload.out(new WritableByteChannel() {
			
			@Override
			public boolean isOpen() {
				return false;
			}
			
			@Override
			public void close() throws IOException {
				
			}
			
			@Override
			public int write(ByteBuffer src) throws IOException {
				
				int readable = result.readableBytes();
				result.writeBytes(src);
				return result.readableBytes() - readable;
			}
		});
		payload.endOutput();

		
		Assert.assertEquals(randomStr.length(), wroteLength);
		
		byte []resultArray = new byte[(int) wroteLength];
		result.readBytes(resultArray);
		Assert.assertEquals(randomStr, new String(resultArray));
	}


	@Test
	public void testNewHeap() throws Exception {
		
		final MemoryPrinter memoryPrinter = new MemoryPrinter(scheduled);

		memoryPrinter.printMemory();

		final int length = 1 << 10;
		int concurrentCount = 10;
		final CountDownLatch latch = new CountDownLatch(concurrentCount);
		
		final ByteBuf byteBuf = directByteBuf(length);

		byteBuf.writeBytes(randomString(length).getBytes());

		byte []dst = new byte[length];
		byteBuf.readBytes(dst);

		memoryPrinter.printMemory();

		for(int i=0;i<concurrentCount;i++){

			Thread current = new Thread(
					new AbstractExceptionLogTask() {
						@Override
						protected void doRun() throws Exception {
							
							try{
								byteBuf.readerIndex(0);
								ByteArrayOutputStream baous = new ByteArrayOutputStream();
								byteBuf.readBytes(baous, length);
							}finally{
								latch.countDown();
							}
						}
			});
			current.start();
			memoryPrinter.printMemory();
			
		}
		
		latch.await();
	}

}
