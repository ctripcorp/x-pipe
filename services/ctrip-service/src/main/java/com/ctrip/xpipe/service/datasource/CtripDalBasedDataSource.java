package com.ctrip.xpipe.service.datasource;

import com.ctrip.datasource.configure.DalDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceDescriptor;

import java.sql.Connection;
import java.sql.SQLException;

public class CtripDalBasedDataSource implements DataSource {

    private static final Logger logger = LoggerFactory.getLogger(CtripDalBasedDataSource.class);

    private javax.sql.DataSource dataSource;

    private DataSourceDescriptor descriptor;

    private static final String DB = "fxxpipedb_dalcluster";

    private DalDataSourceFactory factory = new DalDataSourceFactory();

    private void init() throws Exception {
        dataSource = factory.getOrCreateDataSource(DB);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public DataSourceDescriptor getDescriptor() {
        return descriptor;
    }

    @Override
    public void initialize(DataSourceDescriptor descriptor) {
        this.descriptor = descriptor;
        try {
            init();
        } catch (Exception e) {
            logger.error("[initialize]", e);
        }
    }
}
