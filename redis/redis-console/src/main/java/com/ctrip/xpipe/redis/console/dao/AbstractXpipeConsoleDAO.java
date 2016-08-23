package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.query.DalQueryHandler;

public abstract class AbstractXpipeConsoleDAO {
	protected DalQueryHandler queryHandler = new DalQueryHandler();

}
