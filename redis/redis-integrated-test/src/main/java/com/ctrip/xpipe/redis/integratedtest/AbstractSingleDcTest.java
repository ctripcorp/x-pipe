package com.ctrip.xpipe.redis.integratedtest;


import java.awt.IllegalComponentStateException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Before;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;



/**
 * @author wenchao.meng
 *
 * Jun 15, 2016
 */
public class AbstractSingleDcTest extends AbstractIntegratedTest{
	
	private DcMeta dcMeta;
	
	@Before
	public void beforeAbstractSingleDcTest() throws Exception{

		
		for(DcMeta dcMeta : getXpipeMeta().getDcs().values()){
			if(this.dcMeta != null){
				break;
			}
			for(ClusterMeta clusterMeta : dcMeta.getClusters().values()){
				if(this.dcMeta != null){
					break;
				}
				for(ShardMeta shardMeta : clusterMeta.getShards().values()){
					if(this.dcMeta != null){
						break;
					}
					for(RedisMeta redisMeta : shardMeta.getRedises()){
						if(redisMeta.getMaster()){
							this.dcMeta = dcMeta;
							break;
						}
					}
				}
			}
		}
		
		if(dcMeta == null){
			throw new IllegalComponentStateException("can not find dc with a active redis master");
		}
		
		startDc(dcMeta.getId());
		sleep(1000);
	}
	
	public DcMeta getDcMeta() {
		return dcMeta;
	}
	
	protected RedisMeta getRedisMaster(){
		
		List<RedisMeta> redises = getRedises();
		for(RedisMeta redisMeta : redises){
			if(redisMeta.isMaster()){
				return redisMeta;
			}
		}
		return null;
	}

	private List<RedisMeta> getRedises() {

		for(DcMeta dcMeta : getXpipeMeta().getDcs().values()){
			for(ClusterMeta clusterMeta : dcMeta.getClusters().values()){
				for(ShardMeta shardMeta : clusterMeta.getShards().values()){
					for(RedisMeta redisMeta : shardMeta.getRedises()){
						if(redisMeta.getMaster()){
							return shardMeta.getRedises();
						}
					}
				}
			}
		}
		return null;
	}

	protected List<RedisMeta> getRedisSlaves(){

		List<RedisMeta> result = new LinkedList<>();
		
		for(RedisMeta redisMeta : getRedises()){
			if(!redisMeta.isMaster()){
				result.add(redisMeta);
			}
		}
		return result;
	}
	
	public RedisKeeperServer getRedisKeeperServerActive(){
		
		Map<String, RedisKeeperServer> redisKeeperServers = getRegistry().getComponents(RedisKeeperServer.class);
		
		for(RedisKeeperServer server : redisKeeperServers.values()){
			if(server.getRedisKeeperServerState().isActive()){
				return server;
			}
		}
		return null;
	}
	
	protected void sendMessageToMasterAndTestSlaveRedis() {
		sendRandomMessage(getRedisMaster(), getTestMessageCount());
		sleep(6000);
		assertRedisEquals(getRedisMaster(), getRedisSlaves());
	}

}
