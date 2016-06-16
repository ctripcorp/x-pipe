package com.ctrip.xpipe.redis.integratedtest;


import java.awt.IllegalComponentStateException;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.entity.ClusterMeta;
import com.ctrip.xpipe.redis.keeper.entity.DcMeta;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.entity.ShardMeta;


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
		
		List<Lifecycle> redisKeeperServers = getLifecycleRegistry().getLifecycles(RedisKeeperServer.class);
		
		for(Lifecycle server : redisKeeperServers){
			RedisKeeperServer redisKeeperServer = (RedisKeeperServer) server;
			if(redisKeeperServer.getRedisKeeperServerState().isActive()){
				return redisKeeperServer;
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
