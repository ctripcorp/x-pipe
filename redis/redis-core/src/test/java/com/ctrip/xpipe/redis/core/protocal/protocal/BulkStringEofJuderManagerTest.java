package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.protocal.AbstractBulkStringEoFJudger.BulkStringEofMarkJudger;
import org.junit.Assert;
import org.junit.Test;


/**
 * @author wenchao.meng
 *
 * Dec 23, 2016
 */
public class BulkStringEofJuderManagerTest extends AbstractRedisTest{
	
	@Test
	public void test(){
		
		BulkStringEofJudger judger = null;
		
		judger = BulkStringEofJuderManager.create(("$EOF:" + randomString(BulkStringEofMarkJudger.MARK_LENGTH) + "\r\n").getBytes());
		Assert.assertTrue( judger instanceof BulkStringEofJudger);

		judger = BulkStringEofJuderManager.create(("$EOF:" + randomString(BulkStringEofMarkJudger.MARK_LENGTH + 100) + "\r\n").getBytes());
		Assert.assertTrue( judger instanceof BulkStringEofJudger);

		judger = BulkStringEofJuderManager.create(("$100\r\n").getBytes());
		Assert.assertTrue( judger instanceof BulkStringEofJudger);


		try{
			judger = BulkStringEofJuderManager.create(("$EOF:" + randomString(BulkStringEofMarkJudger.MARK_LENGTH/2) + "\r\n").getBytes());
			Assert.fail();
		}catch(Exception e){
		}
	}

}
