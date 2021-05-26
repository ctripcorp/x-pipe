package com.ctrip.xpipe.service.datasource;

import com.ctrip.datasource.configure.DalDataSourceFactory;
import com.ctrip.platform.dal.dao.configure.FirstAidKit;
import com.ctrip.platform.dal.dao.configure.SerializableDataSourceConfig;
import com.ctrip.platform.dal.dao.datasource.ClusterDynamicDataSource;
import com.ctrip.platform.dal.dao.datasource.ForceSwitchableDataSourceAdapter;
import com.ctrip.xpipe.service.fireman.ForceSwitchableDataSourceHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceDescriptor;
import org.unidal.dal.jdbc.datasource.JdbcDataSourceDescriptor;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author lishanglin
 * date 2021/4/13
 */
public class CtripDynamicDataSource implements DataSource {

    private static final Logger logger = LoggerFactory.getLogger(CtripDynamicDataSource.class);

    private ClusterDynamicDataSource dataSource;

    private DataSourceDescriptor descriptor;

    private DalDataSourceFactory factory;

    private DataSourceConfig config = new DataSourceConfig();

    private void init() throws Exception {
        factory = new DalDataSourceFactory();
        dataSource = (ClusterDynamicDataSource) factory.getOrCreateDataSource(config.getClusterName());
        ForceSwitchableDataSourceHolder.getInstance().setDataSource(new ForceSwitchableDataSourceAdapter(dataSource));
    }

    @Override
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public DataSourceDescriptor getDescriptor() {
        FirstAidKit firstAidKit = dataSource.getFirstAidKit();
        if (firstAidKit instanceof SerializableDataSourceConfig) {
            ((JdbcDataSourceDescriptor)descriptor)
                    .setProperty("url", ((SerializableDataSourceConfig) firstAidKit).getConnectionUrl());
        }

        return descriptor;
    }

    @Override
    public void initialize(DataSourceDescriptor descriptor) {
        this.descriptor = descriptor;
        try {
            init();
        } catch (Throwable e) {
            logger.error("[initialize]", e);
        }
    }

}
