package com.ctrip.xpipe.service.datasource;

import com.ctrip.platform.dal.dao.configure.FirstAidKit;
import com.ctrip.platform.dal.dao.configure.SerializableDataSourceConfig;
import com.ctrip.platform.dal.dao.datasource.ClusterDynamicDataSource;
import com.ctrip.platform.dal.dao.datasource.ForceSwitchableDataSourceAdapter;
import com.ctrip.xpipe.database.ConnectionPoolDesc;
import com.ctrip.xpipe.database.ConnectionPoolHolder;
import com.ctrip.xpipe.datasource.DataSourceFactory;
import com.ctrip.xpipe.service.fireman.ForceSwitchableDataSourceHolder;
import org.apache.tomcat.jdbc.pool.ConnectionPool;
import org.apache.tomcat.jdbc.pool.PoolConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceDescriptor;
import org.unidal.dal.jdbc.datasource.DataSourceException;
import org.unidal.dal.jdbc.datasource.JdbcDataSourceDescriptor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/4/13
 */
public class CtripDynamicDataSource implements DataSource, ConnectionPoolHolder {

    private static final Logger logger = LoggerFactory.getLogger(CtripDynamicDataSource.class);

    private ClusterDynamicDataSource dataSource;

    private DataSourceDescriptor descriptor;

    public CtripDynamicDataSource(DataSourceFactory factory) throws Exception {
        javax.sql.DataSource dataSource = factory.getOrCreateDataSource();
        if (dataSource instanceof ClusterDynamicDataSource) {
            this.dataSource = (ClusterDynamicDataSource) dataSource;
        } else {
            throw new DataSourceException("not a cluster dynamic datasource");
        }
    }

    private void init() {
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

    @Override
    public ConnectionPoolDesc getConnectionPoolDesc() {
        javax.sql.DataSource innerDataSource = dataSource.getSingleDataSource().getDataSource();
        if (innerDataSource instanceof org.apache.tomcat.jdbc.pool.DataSource) {
            ConnectionPool connectionPool = ((org.apache.tomcat.jdbc.pool.DataSource) innerDataSource).getPool();
            return buildConnectionPoolDesc(connectionPool);
        }
        return null;
    }

    private ConnectionPoolDesc buildConnectionPoolDesc(ConnectionPool connectionPool) {
        if (null == connectionPool) return null;

        ConnectionPoolDesc connectionPoolDesc = new ConnectionPoolDesc();
        PoolConfiguration poolConfiguration = connectionPool.getPoolProperties();
        connectionPoolDesc.setUrl(poolConfiguration.getUrl());
        connectionPoolDesc.setMaxWait(poolConfiguration.getMaxWait());
        connectionPoolDesc.setMaxActive(poolConfiguration.getMaxActive());
        connectionPoolDesc.setActive(connectionPool.getActive());
        connectionPoolDesc.setIdle(connectionPool.getIdle());

        String connectionPropertiesString = poolConfiguration.getConnectionProperties();
        String[] connectionProperties = connectionPropertiesString.split(";");
        Map<String, String> connectionPropertyMap = new HashMap<>();
        for (String connectionProperty : connectionProperties) {
            String[] keyAndValue = connectionProperty.split("=");
            if (keyAndValue.length < 2) continue;
            connectionPropertyMap.put(keyAndValue[0], keyAndValue[1]);
        }
        connectionPoolDesc.setConnectionProperties(connectionPropertyMap);

        return connectionPoolDesc;
    }

}
