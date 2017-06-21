package com.ctrip.xpipe.redis.console.query;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;

import com.ctrip.xpipe.redis.console.exception.ServerException;

/**
 * @author shyin
 *
 * Aug 26, 2016
 */
public class DalQueryHandler {
	public <T> T handleQuery(DalQuery<T> query) {
		try {
			return query.doQuery();
		} catch(DalException e) {
			if(e instanceof DalNotFoundException) {
				return null;
			}
			throw new ServerException("Load data failed." + e.getMessage(), e);
		}
	}

}
