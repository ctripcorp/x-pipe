package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Aug 18, 2016
 */
public class ReplicationStoreMetaTest extends AbstractRedisTest{
	
	@Test
	public void testPsync1Version(){
		
		Long keeperBegin = 4089698428721L;
		Long rdbLastKeeperOffset = 4089698428720L;
		Long beginOffset = 1L;
		
		String realData = "{\"beginOffset\":" + beginOffset + ",\"cmdFilePrefix\":\"cmd_aaf0bac6-2323-4fd9-9ff1-3de907b2a59f_\","
				+ "\"keeperBeginOffset\": " + keeperBegin + ",\"keeperRunid\":\"defda76dafa715ea7dc3ccbc9c6406892c203469\",\"keeperState\":\"BACKUP\","
				+ "\"masterAddress\":{\"rawUrl\":\"redis://10.2.24.216:6379\",\"socketAddress\":{\"address\":\"10.2.24.216\",\"port\":6379}},"
				+ "\"masterRunid\":\"aa27eb88752a94c82e66d109c3279b9e3bcd97b3\",\"rdbEofMark\":\"19c9a31ab9aeb916429198e51e7abada9d9ecb62\","
				+ "\"rdbFile\":\"rdb_1487043213489_e0d7641a-da1b-450d-9a28-9d0f5cbd0888\",\"rdbFileSize\":18,\"rdbLastKeeperOffset\":" + rdbLastKeeperOffset + "}";
		ReplicationStoreMeta meta = fromString(realData);
		
		Assert.assertEquals((Long)(beginOffset + (rdbLastKeeperOffset - keeperBegin)), meta.getRdbLastOffset());
		Assert.assertEquals("aa27eb88752a94c82e66d109c3279b9e3bcd97b3", meta.getReplId());
		Assert.assertEquals(null, meta.getReplId2());

		String dese = metaString(meta);
		logger.info("{}", dese);
		ReplicationStoreMeta metaDese = fromString(dese);
		
		Assert.assertEquals(meta, metaDese);
	}
	
	@Test
	public void testEofmark(){

		String realDataBefore = 
				"{\"beginOffset\":2,\"cmdFilePrefix\":\"cmd_6e9ad945-fb0f-46d2-a5df-bbb09d5e555d_\",\"keeperBeginOffset\":2,"
				+ "\"keeperRunid\":\"0123456789012345678901234567890123456789\",\"keeperState\":\"ACTIVE\","
				+ "\"masterAddress\":{\"rawUrl\":\"redis://localhost:6379\",\"socketAddress\":{\"address\":\"127.0.0.1\",\"port\":6379}},"
				+ "\"masterRunid\":\"0e23a43fc0f81ae85109f83bb09b49f13f4b9748\","
				+ "\"rdbFile\":\"rdb_1482676036027_d107ef2f-4a68-4ad7-ad88-b737aab032ce\",\"rdbFileSize\":18,\"rdbLastKeeperOffset\":1}";

		ReplicationStoreMeta meta = fromString(realDataBefore);
		Assert.assertEquals(null, meta.getRdbEofMark());
		Assert.assertEquals(18, meta.getRdbFileSize());


		String realData = 
			"{\"beginOffset\":2,\"cmdFilePrefix\":\"cmd_6e9ad945-fb0f-46d2-a5df-bbb09d5e555d_\",\"keeperBeginOffset\":2,"
			+ "\"keeperRunid\":\"0123456789012345678901234567890123456789\",\"keeperState\":\"ACTIVE\","
			+ "\"masterAddress\":{\"rawUrl\":\"redis://localhost:6379\",\"socketAddress\":{\"address\":\"127.0.0.1\",\"port\":6379}},"
			+ "\"masterRunid\":\"0e23a43fc0f81ae85109f83bb09b49f13f4b9748\","
			+ "\"rdbEofMark\":\"b0c190f35fd4ed46aaf7df075b4b16fb198e0e8e\","
			+ "\"rdbFile\":\"rdb_1482676036027_d107ef2f-4a68-4ad7-ad88-b737aab032ce\",\"rdbFileSize\":18,\"rdbLastKeeperOffset\":1}";
		
		meta = Codec.DEFAULT.decode(realData, ReplicationStoreMeta.class);
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
		String result = metaString(meta);
		logger.info("{}", result);

		ReplicationStoreMeta meta2 = fromString(result);
		
		logger.info("\n{}\n{}", meta, meta2);
		Assert.assertEquals(meta, meta2);
	}
	
	private String metaString(ReplicationStoreMeta meta) {
		return Codec.DEFAULT.encode(meta);
	}

	private ReplicationStoreMeta fromString(String result) {
		return Codec.DEFAULT.decode(result, ReplicationStoreMeta.class);
	}

	@Test
	public void testCopy(){
		
		ReplicationStoreMeta meta = createRandomMeta();
		ReplicationStoreMeta copy = new ReplicationStoreMeta(meta);
		Assert.assertEquals(meta, copy);
	}

	private ReplicationStoreMeta createRandomMeta() {
		
		ReplicationStoreMeta meta = new ReplicationStoreMeta();
		meta.setBeginOffset(2L);
		meta.setCmdFilePrefix("cmd");
		meta.setKeeperRunid(randomString(10));
		meta.setMasterAddress(new DefaultEndPoint("redis://localhost:7777"));
		meta.setReplId(randomString(40));
		meta.setReplId2(randomString(40));
		meta.setSecondReplIdOffset((long) randomInt());
		meta.setRdbFile(randomString(10));
		meta.setRdbFileSize(1000L);
		meta.setKeeperState(KeeperState.ACTIVE);
		return meta;
	}

	@Test
	public void testEncode() {
		ReplicationStoreMeta meta = createRandomMeta();
		Codec.DEFAULT.encode(meta);
		ProxyProtocol protocol = new DefaultProxyConnectProtocolParser().read("PROXY ROUTE TCP://127.0.0.1:6379\r\n");
		meta.setMasterAddress(new DefaultEndPoint("127.0.0.1", 6379, (ProxyConnectProtocol) protocol));
		System.out.println(Codec.DEFAULT.encode(meta));
	}
}
