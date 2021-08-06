package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.payload.AbstractInOutPayload;
import com.ctrip.xpipe.redis.core.protocal.protocal.AbstractBulkStringEoFJudger.BulkStringEofMarkJudger;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午2:35:36
 */
public class BulkStringParserTest extends AbstractRedisProtocolTest{

	private AbstractBulkStringParser bs;

	private ByteBuf result;

	private String content = randomString();

	@Before
	public void beforeBulkStringParserTest(){
		result = directByteBuf();
	}

	@Test
	public void testEOF(){

		String eof = randomString(BulkStringEofMarkJudger.MARK_LENGTH);
		String buff = "$EOF:" + eof + "\r\n" + content + eof;

		for(int i=1; i <= eof.length();i++){

			bs = new RdbBulkStringParser(new TestPayload());
			String []contents = StringUtil.splitByLen(buff, i);
			parse(bs, contents);
			assertResult();
		}
	}

	@Test
	public void testEOFSplit(){
		bs = new RdbBulkStringParser(new TestPayload());
		String eof = randomString(BulkStringEofMarkJudger.MARK_LENGTH);
		String buff = "$EOF:" + eof + "\r\n" + content + eof;
		String []contents = new String[]{buff, randomString()};

		parse(bs, contents);
		assertResult();
		Assert.assertEquals(buff.length(), getTotalReadLen());
	}

	@Test
	public void testNoCRLFEnd(){
		bs = new RdbBulkStringParser(new TestPayload());
		String []contents = new String[]{"$" + content.length(), "\r\n", content, "ab"};

		parse(bs, contents);
		assertResult();

		Assert.assertEquals(content.length(), bs.payload.inputSize());
	}


	@Test
	public void testSplit(){
		bs = new CommandBulkStringParser(new TestPayload());
		String []contents = new String[]{"$" + content.length(), "\r\n", content, "\r\n"};
		ByteBuf []byteBufs = new ByteBuf[contents.length];

		for(int i = 0; i< contents.length;i++){

			byteBufs[i] = directByteBuf();
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
		bs = new CommandBulkStringParser(new TestPayload());
		String data = "$" +content.length() + "\r\n" + content + "\r\n";
		ByteBuf byteBuf = directByteBuf(1024);
		byteBuf.writeBytes(data.getBytes());
		bs.read(byteBuf);

		assertResult();
	}


	class TestPayload  extends AbstractInOutPayload implements InOutPayload{

		@Override
		public int doIn(ByteBuf byteBuf) {

			int current = byteBuf.readableBytes();
			result.writeBytes(byteBuf);
			return current - byteBuf.readableBytes();
		}

		@Override
		public long doOut(WritableByteChannel writableByteChannel) throws IOException {
			return 0;
		}

		@Override
		protected void doTruncate(int reduceLen) throws IOException {
			int writerIndex = result.writerIndex();
			result.writerIndex(writerIndex - reduceLen);
		}
	}
}
