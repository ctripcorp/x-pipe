package com.ctrip.xpipe.datasource;

import javax.sql.DataSource;

public interface DataSourceFactory {

    DataSource getOrCreateDataSource() throws Exception;

}
