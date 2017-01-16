package com.ctrip.xpipe.redis.integratedtest.keeper;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.utils.IpUtils;

import redis.clients.jedis.Jedis;

/**
 * @author wenchao.meng
 *
 * Aug 17, 2016
 */
public class KeeperSingleDcVersionTest extends AbstractKeeperIntegratedSingleDc{
	
	private String addr_2_8_19_str = System.getProperty("REDIS_2_8_19", "127.0.0.1:2819");
	
	private InetSocketAddress addr_2_8_19;
	
	@Before
	public void beforeKeeperSingleDcVersionTest(){
		
		addr_2_8_19 = IpUtils.parseSingle(addr_2_8_19_str);
		
		makeRedisMaster(addr_2_8_19);
		flushAll(addr_2_8_19);
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
