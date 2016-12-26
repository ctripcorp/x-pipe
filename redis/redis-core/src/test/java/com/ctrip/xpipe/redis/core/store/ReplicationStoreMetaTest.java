package com.ctrip.xpipe.redis.core.store;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.meta.KeeperState;

/**
 * @author wenchao.meng
 *
 * Aug 18, 2016
 */
public class ReplicationStoreMetaTest extends AbstractRedisTest{
	
	@Test
	public void testEofmark(){

		String realDataBefore = 
				"{\"beginOffset\":2,\"cmdFilePrefix\":\"cmd_6e9ad945-fb0f-46d2-a5df-bbb09d5e555d_\",\"keeperBeginOffset\":2,"
				+ "\"keeperRunid\":\"0123456789012345678901234567890123456789\",\"keeperState\":\"ACTIVE\","
				+ "\"masterAddress\":{\"rawUrl\":\"redis://localhost:6379\",\"socketAddress\":{\"address\":\"127.0.0.1\",\"port\":6379}},"
				+ "\"masterRunid\":\"0e23a43fc0f81ae85109f83bb09b49f13f4b9748\","
				+ "\"rdbFile\":\"rdb_1482676036027_d107ef2f-4a68-4ad7-ad88-b737aab032ce\",\"rdbFileSize\":18,\"rdbLastKeeperOffset\":1}";

		ReplicationStoreMeta meta = JSON.parseObject(realDataBefore, ReplicationStoreMeta.class);
		Assert.assertEquals(null, meta.getRdbEofMark());
		Assert.assertEquals(18, meta.getRdbFileSize());


		String realData = 
			"{\"beginOffset\":2,\"cmdFilePrefix\":\"cmd_6e9ad945-fb0f-46d2-a5df-bbb09d5e555d_\",\"keeperBeginOffset\":2,"
			+ "\"keeperRunid\":\"0123456789012345678901234567890123456789\",\"keeperState\":\"ACTIVE\","
			+ "\"masterAddress\":{\"rawUrl\":\"redis://localhost:6379\",\"socketAddress\":{\"address\":\"127.0.0.1\",\"port\":6379}},"
			+ "\"masterRunid\":\"0e23a43fc0f81ae85109f83bb09b49f13f4b9748\","
			+ "\"rdbEofMark\":\"b0c190f35fd4ed46aaf7df075b4b16fb198e0e8e\","
			+ "\"rdbFile\":\"rdb_1482676036027_d107ef2f-4a68-4ad7-ad88-b737aab032ce\",\"rdbFileSize\":18,\"rdbLastKeeperOffset\":1}";
		
		meta = JSON.parseObject(realData, ReplicationStoreMeta.class);
		Assert.assertEquals("b0c190f35fd4ed46aaf7df075b4b16fb198e0e8e", meta.getRdbEofMark());
		Assert.assertEquals(18, meta.getRdbFileSize());
		
	}
	
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
	public void testJackson(){
		
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
		meta.setKeeperState(KeeperState.ACTIVE);
		meta.setRdbLastKeeperOffset(10000L);
		return meta;
	}

}
