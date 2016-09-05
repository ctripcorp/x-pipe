package com.ctrip.xpipe.netty;

import java.net.InetSocketAddress;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientFactory;
import com.ctrip.xpipe.pool.XpipeObjectPool;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class NettyPoolUtil {
	
	public static SimpleObjectPool<NettyClient>  createNettyPool(InetSocketAddress target){
		
		try {
			XpipeObjectPool<NettyClient> xpipeObjectPool = new XpipeObjectPool<>(new NettyClientFactory(target));
			xpipeObjectPool.initialize();
			xpipeObjectPool.start();
			return xpipeObjectPool;
		} catch (Exception e) {
			throw new IllegalStateException("[createNettyPool]" + target, e);
		}
	}

}
