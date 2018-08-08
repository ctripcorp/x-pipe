package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigGetCommand.ConfigGetDisklessSync;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigGetCommand.ConfigGetDisklessSyncDelay;
import com.ctrip.xpipe.utils.IpUtils;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author wenchao.meng
 *
 * Aug 17, 2016
 */
public class KeeperSingleDcVersionTest extends AbstractKeeperIntegratedSingleDc{
	
	private String addr_2_8_19_str = System.getProperty("REDIS_2_8_19", "127.0.0.1:2819");
	
	private InetSocketAddress addr_2_8_19;
	
	@Override
	protected void doBeforeIntegratedTest() throws Exception {
		super.doBeforeIntegratedTest();

		addr_2_8_19 = IpUtils.parseSingle(addr_2_8_19_str);

		checkAndPrepareRedis(addr_2_8_19, "2.8.19");
	}
	
	@Test
	public void test2_8_19Tocurrent() throws Exception{
		
		
		RedisMeta newMaster = new RedisMeta().setIp(addr_2_8_19.getHostString()).setPort(addr_2_8_19.getPort());
		setKeeperState(activeKeeper, KeeperState.ACTIVE, addr_2_8_19.getHostString(), addr_2_8_19.getPort());
		
		sleep(2000);
		
		sendMesssageToMasterAndTest(100, newMaster, slaves);
	}

	@Test
	public void testcurrentTo2_8_19() throws IOException{

		makeRedisSlaveof(addr_2_8_19, activeKeeper.getIp(), activeKeeper.getPort());
		
		sleep(2000);
		
		slaves.add(new RedisMeta().setIp(addr_2_8_19.getHostString()).setPort(addr_2_8_19.getPort()));

		sendMesssageToMasterAndTest(100, redisMaster, slaves);
		
	}

	private void checkAndPrepareRedis(InetSocketAddress redisAddr, String version) throws Exception {
		
		if(!checkVersion(redisAddr, version)){
			throw new IllegalStateException("redis(" + redisAddr +") version not right, expected:" + version);
		}
		
		SimpleObjectPool<NettyClient>  clientPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(redisAddr));
		Boolean diskLess = new ConfigGetDisklessSync(clientPool, scheduled).execute().get();
		if(diskLess){
			int diskLessSyncDelay = new ConfigGetDisklessSyncDelay(clientPool, scheduled).execute().get();
			if(diskLessSyncDelay != 0){
				throw new IllegalArgumentException("redis(" + redisAddr + ")expected diskless sync delay 0, but:" + diskLessSyncDelay);
			}
		}
		makeRedisMaster(redisAddr);
		flushAll(redisAddr);
	}

	private void makeRedisMaster(InetSocketAddress addr) {
		try(Jedis jedis = createJedis(addr)){
			jedis.slaveofNoOne();
		}
	}

	private void makeRedisSlaveof(InetSocketAddress redis, String slaveOfIp, int slaveOfPort) {
		
		try(Jedis jedis = createJedis(redis)){
			jedis.slaveof(slaveOfIp, slaveOfPort);
		}
	}

	
	private void flushAll(InetSocketAddress addr) {
		try(Jedis jedis = createJedis(addr)){
			jedis.flushAll();
		}
	}

	@Override
	protected int getInitSleepMilli() {
		return 1000;
	}
}
