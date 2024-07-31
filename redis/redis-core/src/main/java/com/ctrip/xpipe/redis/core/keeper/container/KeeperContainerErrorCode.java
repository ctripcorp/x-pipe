package com.ctrip.xpipe.redis.core.keeper.container;

/**
 * @author wenchao.meng
 *
 * Aug 11, 2016
 */
public enum KeeperContainerErrorCode {
	
	INTERNAL_EXCEPTION,
	KEEPER_ALREADY_EXIST,
	KEEPER_NOT_EXIST,
	KEEPER_NOT_STARTED,
	KEEPER_ALREADY_STARTED,
	KEEPER_ALREADY_STOPPED,
	KEEPER_ALREADY_DELETED
}
