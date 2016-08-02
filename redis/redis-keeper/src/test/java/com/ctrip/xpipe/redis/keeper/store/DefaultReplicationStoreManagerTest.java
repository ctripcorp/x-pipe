package com.ctrip.xpipe.redis.keeper.store;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import java.io.File;

import org.junit.Test;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * @author marsqing
 *
 *         Jun 1, 2016 9:47:12 AM
 */
public class DefaultReplicationStoreManagerTest extends AbstractRedisKeeperTest{

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

		MetaStore metaStore = newCurrentStore.getMetaStore();
		metaStore.setMasterAddress(new DefaultEndPoint("redis://127.0.0.1:6379"));
		newCurrentStore.beginRdb("masterRunid", 0, 100);

		ByteBuf cmdBuf = Unpooled.buffer();
		cmdBuf.writeByte(9);
		newCurrentStore.getCommandStore().appendCommands(cmdBuf);

		DefaultReplicationStoreManager mgr2 = new DefaultReplicationStoreManager(clusterId, shardId, baseDir);
		assertEquals(metaStore.getMasterRunid(), mgr2.getCurrent().getMetaStore().getMasterRunid());
		assertEquals(metaStore.getKeeperBeginOffset(), mgr2.getCurrent().getMetaStore().getKeeperBeginOffset());
		assertEquals(metaStore.getMasterAddress(), mgr2.getCurrent().getMetaStore().getMasterAddress());
		assertEquals(metaStore.beginOffset(), mgr2.getCurrent().getMetaStore().beginOffset());
	}

}
