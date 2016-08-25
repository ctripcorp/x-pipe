package com.ctrip.xpipe.redis.console.query;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;

import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;

public class DalQueryHandler {
	public <T> T handleQuery(DalQuery<T> query) {
		try {
			return query.doQuery();
		} catch(DalException e) {
			if(e instanceof DalNotFoundException) {
				throw new DataNotFoundException("Data not found.",e);
			}
			throw new ServerException("Load data failed.", e);
		}
	}
	
	public <T> T tryGet(DalQuery<T> query) {
		try {
			return query.doQuery();
		} catch(DalException e) {
			if(e instanceof DalNotFoundException) {
				return null;
			}
			throw new ServerException("Try get data fail.", e);
		}
	}

}
