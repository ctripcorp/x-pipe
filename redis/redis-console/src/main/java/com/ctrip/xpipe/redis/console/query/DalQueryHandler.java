package com.ctrip.xpipe.redis.console.query;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;

import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;

public class DalQueryHandler {
	public <T> T handleQuery(DalQuery<T> query) {
		try {
			return query.doQuery();
		} catch(DalNotFoundException e) {
			throw new DataNotFoundException("Data not found.", e);
		} catch(DalException e) {
			throw new ServerException("Load data failed.", e);
		}
	}

}
