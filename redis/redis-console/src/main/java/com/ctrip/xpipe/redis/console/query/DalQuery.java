package com.ctrip.xpipe.redis.console.query;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;

public interface DalQuery<T> {
	T doQuery() throws DalNotFoundException, DalException;
}
