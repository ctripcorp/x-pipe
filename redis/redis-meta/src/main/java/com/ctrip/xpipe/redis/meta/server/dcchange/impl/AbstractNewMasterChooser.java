package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.meta.server.dcchange.NewMasterChooser;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public abstract class AbstractNewMasterChooser implements NewMasterChooser{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	public static final int CHECK_NEW_MASTER_TIMEOUT_SECONDS = Integer.parseInt(System.getProperty("CHECK_NEW_MASTER_TIMEOUT_SECONDS", "2")); 
	
	protected XpipeNettyClientKeyedObjectPool keyedObjectPool;
	
	protected RedisMeta newMaster = null;

	public AbstractNewMasterChooser(XpipeNettyClientKeyedObjectPool keyedObjectPool) {
		this.keyedObjectPool = keyedObjectPool;
	}

	
	public RedisMeta getLastChoosenMaster(){
		return newMaster;
	}

	@Override
	public RedisMeta choose(List<RedisMeta> redises) {
		
		List<RedisMeta> masters = getMasters(redises);
		if(masters.size() == 0){
			newMaster = doChoose(redises);
		}else if(masters.size() == 1){
			logger.info("[choose][already has master]{}", masters);
			newMaster = masters.get(0);
		}else{
			throw new IllegalStateException("multi master there, can not choose a new master " + masters);
		}
		return newMaster;
	}

	protected List<RedisMeta> getMasters(List<RedisMeta> allRedises) {
		
		List<RedisMeta> result = new LinkedList<>();
		
		for(RedisMeta redisMeta : allRedises){
			if(isMaster(redisMeta)){
				result.add(redisMeta);
			}
		}
		
		return result;
	}

	protected boolean isMaster(RedisMeta redisMeta) {
		
		try {
			SimpleObjectPool<NettyClient> clientPool = keyedObjectPool.getKeyPool(new InetSocketAddress(redisMeta.getIp(), redisMeta.getPort()));
			Role role = new RoleCommand(clientPool).execute().get(CHECK_NEW_MASTER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			return SERVER_ROLE.MASTER == role.getServerRole();
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			logger.error("[isMaster]" + redisMeta, e);
		}
		return false;
	}

	protected abstract RedisMeta doChoose(List<RedisMeta> redises);

}
