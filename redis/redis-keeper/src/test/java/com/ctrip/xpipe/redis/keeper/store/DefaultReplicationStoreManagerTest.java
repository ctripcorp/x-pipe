package com.ctrip.xpipe.redis.keeper.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import java.io.File;

import org.junit.Test;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @author marsqing
 *
 *         Jun 1, 2016 9:47:12 AM
 */
public class DefaultReplicationStoreManagerTest extends AbstractRedisTest {

	@Test
	public void test() throws Exception {
		File baseDir = new File(getTestFileDir());
		String clusterId = "cluster1";
		String shardId = "shard1";
		DefaultReplicationStoreManager mgr = new DefaultReplicationStoreManager(clusterId, shardId, baseDir);

		ReplicationStore currentStore = mgr.getCurrent();
		assertNull(currentStore);

		currentStore = mgr.create();

		assertEquals(clusterId, mgr.getClusterName());
		assertEquals(shardId, mgr.getShardName());
		assertEquals(currentStore, mgr.getCurrent());

		ReplicationStore newCurrentStore = mgr.create();
		assertEquals(newCurrentStore, mgr.getCurrent());
		assertNotEquals(currentStore, mgr.getCurrent());

		newCurrentStore.setMasterAddress(new DefaultEndPoint("redis://127.0.0.1:6379"));
		newCurrentStore.beginRdb("masterRunid", 0, 100);

		ByteBuf cmdBuf = Unpooled.buffer();
		cmdBuf.writeByte(9);
		newCurrentStore.appendCommands(cmdBuf);

		DefaultReplicationStoreManager mgr2 = new DefaultReplicationStoreManager(clusterId, shardId, baseDir);
		assertEquals(newCurrentStore.getMasterRunid(), mgr2.getCurrent().getMasterRunid());
		assertEquals(newCurrentStore.getKeeperBeginOffset(), mgr2.getCurrent().getKeeperBeginOffset());
		assertEquals(newCurrentStore.getMasterAddress(), mgr2.getCurrent().getMasterAddress());
		assertEquals(newCurrentStore.beginOffset(), mgr2.getCurrent().beginOffset());
	}

}
