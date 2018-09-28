package com.ctrip.xpipe.netty;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientPool;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class NettyPoolUtil {
	
	public static SimpleObjectPool<NettyClient>  createNettyPool(Endpoint target){
		
		try {
			XpipeNettyClientPool xpipeObjectPool = new XpipeNettyClientPool(target);
			xpipeObjectPool.initialize();
			xpipeObjectPool.start();
			return xpipeObjectPool;
		} catch (Exception e) {
			throw new IllegalStateException("[createNettyPool]" + target, e);
		}
	}

}
