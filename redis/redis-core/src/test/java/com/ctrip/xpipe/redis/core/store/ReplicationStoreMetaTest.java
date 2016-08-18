package com.ctrip.xpipe.redis.core.store;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;

/**
 * @author wenchao.meng
 *
 * Aug 18, 2016
 */
public class ReplicationStoreMetaTest extends AbstractRedisTest{
	
	@Test
	public void testEquals(){
		
		ReplicationStoreMeta meta = createRandomMeta();

		ReplicationStoreMeta meta2 = SerializationUtils.clone(meta);
		Assert.assertEquals(meta, meta2);

		ReplicationStoreMeta meta3 = new ReplicationStoreMeta(meta);
		Assert.assertEquals(meta, meta3);
		
		meta3.setBeginOffset(meta3.getBeginOffset()+1);
		Assert.assertNotEquals(meta, meta3);

	}
	

	@Test
	public void testJson(){
		
		ReplicationStoreMeta meta = createRandomMeta();
		String result = JSON.toJSONString(meta);
		logger.info("{}", result);

		ReplicationStoreMeta meta2 = JSON.parseObject(result, ReplicationStoreMeta.class);
		Assert.assertEquals(meta, meta2);
		

	}

	private ReplicationStoreMeta createRandomMeta() {
		
		ReplicationStoreMeta meta = new ReplicationStoreMeta();
		meta.setBeginOffset(2L);
		meta.setCmdFilePrefix("cmd");
		meta.setKeeperBeginOffset(1L);
		meta.setKeeperRunid(randomString(10));
		meta.setMasterAddress(new DefaultEndPoint("redis://localhost:7777"));
		meta.setMasterRunid(randomString(10));
		meta.setRdbFile(randomString(10));
		meta.setRdbFileSize(1000L);
		meta.setRdbLastKeeperOffset(10000L);
		return meta;
	}

}
