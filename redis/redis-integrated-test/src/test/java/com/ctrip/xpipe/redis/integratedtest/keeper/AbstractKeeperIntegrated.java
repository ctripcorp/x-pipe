package com.ctrip.xpipe.redis.integratedtest.keeper;

import java.net.InetSocketAddress;

import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractKeeperCommand.KeeperSetStateCommand;
import com.ctrip.xpipe.redis.integratedtest.AbstractIntegratedTest;

/**
 * @author wenchao.meng
 *
 * Aug 17, 2016
 */
public abstract class AbstractKeeperIntegrated extends AbstractIntegratedTest{

	@Override
	protected String getRedisTemplate() {
		return "conf/redis_raw.conf";
	}

	protected KeeperMeta getKeeperActive(RedisMeta redisMeta) {
		
		for(KeeperMeta keeper : redisMeta.parent().getKeepers()){
			if(keeper.isActive()){
				return keeper;
			}
		}
		return null;
	}

	protected void setKeeperState(KeeperMeta keeperMeta, KeeperState keeperState, String ip, Integer port) throws Exception {
		
		
		KeeperSetStateCommand command = new KeeperSetStateCommand(
				NettyPoolUtil.createNettyPool(new InetSocketAddress(keeperMeta.getIp(), keeperMeta.getPort())),
				keeperState, new InetSocketAddress(ip, port));
		command.execute().sync();
	}
}
