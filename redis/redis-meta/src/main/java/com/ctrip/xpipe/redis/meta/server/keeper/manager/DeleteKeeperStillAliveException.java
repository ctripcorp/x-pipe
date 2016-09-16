package com.ctrip.xpipe.redis.meta.server.keeper.manager;


import java.util.List;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.exception.MetaServerException;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
public class DeleteKeeperStillAliveException extends MetaServerException{
	
	private static final long serialVersionUID = 1L;
		
	public DeleteKeeperStillAliveException(List<KeeperMeta> surviveKeepers, KeeperMeta currentKeeper){
		super(String.format("survive keepers:%s: current keeper:%s", surviveKeepers, currentKeeper));
		setOnlyLogMessage(true);
	}
}
