package com.ctrip.xpipe.redis.protocal.protocal;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.api.payload.InOutPayload;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;


/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午2:35:36
 */
public class BulkStringTest extends AbstractRedisProtocolTest{
	
	private ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
	
	private BulkStringParser bs = new BulkStringParser(new TestPayload());
	
	private ByteBuf result = allocator.directBuffer();
	
	private String content = "0123456789abcdefg";

	@Test
	public void testNoCRLFEnd(){

		String []contents = new String[]{"$" + content.length(), "\r\n", content, "ab"};
		ByteBuf []byteBufs = new ByteBuf[contents.length];
		
		for(int i = 0; i< contents.length;i++){
			
			byteBufs[i] = allocator.buffer();
			byteBufs[i].writeBytes(contents[i].getBytes());
		}
		
		for(ByteBuf byteBuf : byteBufs){
			bs.read(byteBuf);
		}
		
		assertResult();
		
		int lastIndex = contents.length - 1;
		Assert.assertEquals(contents[lastIndex].length(), byteBufs[lastIndex].readableBytes());

	}
	
	@Test
	public void testSplit(){
		
		String []contents = new String[]{"$" + content.length(), "\r\n", content, "\r\n"};
		ByteBuf []byteBufs = new ByteBuf[contents.length];
		
		for(int i = 0; i< contents.length;i++){
			
			byteBufs[i] = allocator.buffer();
			byteBufs[i].writeBytes(contents[i].getBytes());
		}
		
		for(ByteBuf byteBuf : byteBufs){
			bs.read(byteBuf);
		}
		
		assertResult();
		
	}
	
	private void assertResult() {
		
		Assert.assertEquals(content.length(), result.readableBytes());
		byte [] resultBytes = new byte[result.readableBytes()];
		result.readBytes(resultBytes);
		Assert.assertArrayEquals(content.getBytes(), resultBytes);
		
	}

	@Test
	public void testRight() throws IOException{
		
		String data = "$" +content.length() + "\r\n" + content + "\r\n";
		ByteBuf byteBuf = allocator.buffer(1024);
		byteBuf.writeBytes(data.getBytes());
		bs.read(byteBuf);
		
		assertResult();
	}

	
	class TestPayload implements InOutPayload{

		@Override
		public void startInput() {
		}

		@Override
		public int in(ByteBuf byteBuf) {
			
			int current = byteBuf.readableBytes();
			result.writeBytes(byteBuf);
			return current - byteBuf.readableBytes();
		}

		@Override
		public void endInput() {
			
		}

		@Override
		public void startOutput() {
		}
		
		@Override
		public long out(WritableByteChannel writableByteChannel) throws IOException {
			return 0;
		}

		@Override
		public void endOutput() {
			
		}
	}
	
	
}
