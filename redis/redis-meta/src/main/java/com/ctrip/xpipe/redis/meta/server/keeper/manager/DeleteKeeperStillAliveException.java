package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.exception.MetaServerException;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
public class DeleteKeeperStillAliveException extends MetaServerException{
	
	private static final long serialVersionUID = 1L;
		
	public DeleteKeeperStillAliveException(KeeperMeta currentKeeper){
		super(String.format("current keeper still alive:%s:%d", currentKeeper.getIp(), currentKeeper.getPort()));
		setOnlyLogMessage(true);
	}
}
