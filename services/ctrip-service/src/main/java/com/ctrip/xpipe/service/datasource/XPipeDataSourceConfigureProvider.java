package com.ctrip.xpipe.redis.console.ds;

import com.ctrip.platform.dal.dao.configure.AbstractDataSourceConfigure;
import com.ctrip.platform.dal.dao.configure.IDataSourceConfigure;
import com.ctrip.platform.dal.dao.configure.IDataSourceConfigureProvider;
import org.unidal.dal.jdbc.datasource.DataSourceDescriptor;

public class XPipeDataSourceConfigureProvider implements IDataSourceConfigureProvider {

    private DataSourceDescriptor descriptor;

    public XPipeDataSourceConfigureProvider(DataSourceDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public IDataSourceConfigure getDataSourceConfigure() {
        return new XPipeDataSourceConfigure();
    }

    @Override
    public IDataSourceConfigure forceLoadDataSourceConfigure() {
        return new XPipeDataSourceConfigure();
    }

    public class XPipeDataSourceConfigure extends AbstractDataSourceConfigure {

        @Override
        public String getUserName() {
            return descriptor.getProperty("user", "root");
        }

        @Override
        public String getPassword() {
            return descriptor.getProperty("password", "password");
        }

        @Override
        public String getConnectionUrl() {
            return descriptor.getProperty("url", "");
        }

        @Override
        public String getDriverClass() {
            return descriptor.getProperty("driver", "mysql");
        }

        @Override
        public String getHostName() {
            return descriptor.getProperty("host", "localhost");
        }

        @Override
        public Integer getPort() {
            return descriptor.getIntProperty("port", 3306);
        }

        @Override
        public String getDBName() {
            return descriptor.getId();
        }
    }
}
