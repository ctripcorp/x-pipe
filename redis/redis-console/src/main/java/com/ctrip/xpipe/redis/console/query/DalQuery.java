package com.ctrip.xpipe.redis.console.query;

import org.unidal.dal.jdbc.DalException;

public interface DalQuery<T> {
	T doQuery() throws DalException;
}
