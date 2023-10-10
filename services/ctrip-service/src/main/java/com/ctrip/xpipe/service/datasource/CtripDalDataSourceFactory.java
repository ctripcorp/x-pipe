package com.ctrip.xpipe.service.datasource;

import com.ctrip.datasource.configure.DalDataSourceFactory;
import com.ctrip.xpipe.datasource.DataSourceFactory;

import javax.sql.DataSource;

public class CtripDalDataSourceFactory implements DataSourceFactory {
    private final DataSourceConfig config = new DataSourceConfig();
    private final DalDataSourceFactory factory = new DalDataSourceFactory();

    private volatile DataSource dataSource;

    @Override
    public DataSource getOrCreateDataSource() throws Exception {
        if (dataSource == null) {
            synchronized (this) {
                if (dataSource == null) {
                    dataSource = factory.getOrCreateDataSource(config.getClusterName());
                }
            }
        }
        return dataSource;
    }
}
