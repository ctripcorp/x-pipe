package com.ctrip.xpipe.redis.core.protocal.protocal;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.AbstractBulkStringEoFJudger.BulkStringEofMarkJudger;
import com.ctrip.xpipe.redis.core.protocal.protocal.AbstractBulkStringEoFJudger.BulkStringLengthEofJudger;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringEofJudger.JudgeResult;

import io.netty.buffer.Unpooled;

/**
 * @author wenchao.meng
 *
 *         Dec 22, 2016
 */
public class BulkStringEofJudgerTest extends AbstractRedisTest {

	@Test
	public void testEofMarkSmall() {

		byte[] eofmark = randomString(RedisClientProtocol.RUN_ID_LENGTH).getBytes();
		
		BulkStringEofMarkJudger judger = new BulkStringEofMarkJudger(eofmark);

		for (int i = 0; i < (1<<20); i++) {
			
			JudgeResult result = judger.end(Unpooled.wrappedBuffer(randomString(1).getBytes()));
			Assert.assertFalse(result.isEnd());
			Assert.assertEquals(1, result.getReadLen());
		}
		for(int i=0;i<eofmark.length;i++){

			JudgeResult result = judger.end(Unpooled.wrappedBuffer(new byte[]{eofmark[i]}));
			Assert.assertEquals(1, result.getReadLen());
			if(i<eofmark.length - 1){
				Assert.assertFalse(result.isEnd());
			}else{
				Assert.assertTrue(result.isEnd());
			}
		}
	}

	@Test
	public void testEofMarkBig() {
		
		byte[] eofmark = randomString(RedisClientProtocol.RUN_ID_LENGTH).getBytes();
		
		BulkStringEofMarkJudger judger = new BulkStringEofMarkJudger(eofmark);
		
		String data = randomString();
		JudgeResult result = judger.end(Unpooled.wrappedBuffer(data.getBytes()));
		
		Assert.assertFalse(result.isEnd());
		Assert.assertEquals(data.length(), result.getReadLen());

		String real = randomString() + new String(eofmark);
		result = judger.end(Unpooled.wrappedBuffer(real.getBytes()));
		
		Assert.assertTrue(result.isEnd());
		Assert.assertEquals(real.length(), result.getReadLen());
	}
	
	@Test
	public void testLen(){
		
		int expectedLen = 1 << 20;
		BulkStringLengthEofJudger judger = new BulkStringLengthEofJudger(expectedLen);
		
		for(int i=0;i<expectedLen -1 ;i++){
			
			JudgeResult result = judger.end(Unpooled.wrappedBuffer("a".getBytes()));
			Assert.assertFalse(result.isEnd());
			Assert.assertEquals(1, result.getReadLen());
		}
		
		JudgeResult result = judger.end(Unpooled.wrappedBuffer("a".getBytes()));
		Assert.assertTrue(result.isEnd());
		Assert.assertEquals(1, result.getReadLen());

		result = judger.end(Unpooled.wrappedBuffer("a".getBytes()));
		Assert.assertTrue(result.isEnd());
		Assert.assertEquals(0, result.getReadLen());

		try{
			result = judger.end(Unpooled.wrappedBuffer("a".getBytes()));
			Assert.fail();
		}catch(Exception e){
			
		}
	}

}
