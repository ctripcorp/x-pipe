package com.ctrip.xpipe.redis.console.query;

import org.unidal.dal.jdbc.DalException;

/**
 * @author shyin
 *
 * Aug 26, 2016
 */
public interface DalQuery<T> {
	T doQuery() throws DalException;
}
