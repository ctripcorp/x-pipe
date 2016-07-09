package com.ctrip.xpipe.redis.console.dao;


import com.ctrip.xpipe.redis.core.dao.DaoException;
import com.ctrip.xpipe.redis.core.dao.MetaUpdateOperation;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public interface ConsoleUpdateOperation extends MetaUpdateOperation{
	
	
	boolean updateActiveDc(String clusterId, String activeDc) throws DaoException;
}	

