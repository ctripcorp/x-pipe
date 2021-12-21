package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *
 * Nov 13, 2016
 */
public class PrimaryDcKeeperMasterChooserAlgorithmTest extends AbstractDcKeeperMasterChooserTest{
	
	private PrimaryDcKeeperMasterChooserAlgorithm primaryAlgorithm;
	
	private List<RedisMeta> redises;
	
	@Before
	public void befoePrimaryDcKeeperMasterChooserTest() throws Exception{
		
		primaryAlgorithm =  new PrimaryDcKeeperMasterChooserAlgorithm(clusterDbId, shardDbId,
				dcMetaCache, currentMetaManager, getXpipeNettyClientKeyedObjectPool(), 1, scheduled);
		redises = new LinkedList<>();
		int port1 = randomPort();
		redises.add(new RedisMeta().setIp("localhost").setPort(port1));
		redises.add(new RedisMeta().setIp("localhost").setPort(randomPort(Sets.newHashSet(port1))));
		when(dcMetaCache.getShardRedises(clusterDbId, shardDbId)).thenReturn(redises);
	}
	
	@Test
	public void testNoneMaster(){

		when(currentMetaManager.getKeeperMaster(clusterDbId, shardDbId)).thenReturn(null);
		Assert.assertEquals(new Pair<>(redises.get(0).getIp(), redises.get(0).getPort()) ,primaryAlgorithm.choose());

		when(currentMetaManager.getKeeperMaster(clusterDbId, shardDbId)).thenReturn(new Pair<>(redises.get(1).getIp(), redises.get(1).getPort()));
		Assert.assertEquals(new Pair<>(redises.get(1).getIp(), redises.get(1).getPort()) ,primaryAlgorithm.choose());

	}

	@Test
	public void testOneMaster() throws Exception{

		SlaveRole role = new SlaveRole(SERVER_ROLE.MASTER, "localhost", randomPort(), MASTER_STATE.REDIS_REPL_CONNECT, 0L);
		RedisMeta chosen = redises.get(0);
		startServer(chosen.getPort(), ByteBufUtils.readToString(role.format()));
		

		when(currentMetaManager.getKeeperMaster(clusterDbId, shardDbId)).thenReturn(null);
		Assert.assertEquals(new Pair<String, Integer>(chosen.getIp(), chosen.getPort()), primaryAlgorithm.choose());
		for(RedisMeta redisMeta : redises){
			
			when(currentMetaManager.getKeeperMaster(clusterDbId, shardDbId)).thenReturn(new Pair<String, Integer>(redisMeta.getIp(), redisMeta.getPort()));
			Assert.assertEquals(new Pair<String, Integer>(chosen.getIp(), chosen.getPort()), primaryAlgorithm.choose());
		}
	}
	
	@Test
	public void testLongConnection() throws Exception{

		SlaveRole role = new SlaveRole(SERVER_ROLE.MASTER, "localhost", randomPort(), MASTER_STATE.REDIS_REPL_CONNECT, 0L);
		RedisMeta chosen = redises.get(0);
		Server server = startServer(chosen.getPort(), ByteBufUtils.readToString(role.format()));
		
		Assert.assertEquals(0, server.getConnected());
		
		for(int i=0;i<10;i++){
			
			primaryAlgorithm.choose();
			Assert.assertEquals(1, server.getConnected());
		}

	}

	@Test
	public void testMultiMaster() throws Exception{

		SlaveRole role = new SlaveRole(SERVER_ROLE.MASTER, "localhost", randomPort(), MASTER_STATE.REDIS_REPL_CONNECT, 0L);
		
		for(RedisMeta redisMeta : redises){
			startServer(redisMeta.getPort(), ByteBufUtils.readToString(role.format()));
		}
		
		when(currentMetaManager.getKeeperMaster(clusterDbId, shardDbId)).thenReturn(null);
		Assert.assertEquals(new Pair<String, Integer>(redises.get(0).getIp(), redises.get(0).getPort()), primaryAlgorithm.choose());
		
		for(RedisMeta redisMeta : redises){
			
			Pair<String, Integer> currentMaster = new Pair<String, Integer>(redisMeta.getIp(), redisMeta.getPort());
			when(currentMetaManager.getKeeperMaster(clusterDbId, shardDbId)).thenReturn(currentMaster);
			Assert.assertEquals(currentMaster, primaryAlgorithm.choose());
		}
	}
}
