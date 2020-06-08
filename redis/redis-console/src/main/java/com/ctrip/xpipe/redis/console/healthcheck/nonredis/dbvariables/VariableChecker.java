package com.ctrip.xpipe.redis.console.healthcheck.nonredis.dbvariables;

import org.unidal.dal.jdbc.datasource.DataSource;

public interface VariableChecker {

    void check(DataSource dataSource);

}
