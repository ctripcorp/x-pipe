package com.ctrip.xpipe.service.datasource;

import com.ctrip.platform.dal.dao.configure.AbstractDataSourceConfigure;
import com.ctrip.platform.dal.dao.configure.IDataSourceConfigure;
import com.ctrip.platform.dal.dao.configure.IDataSourceConfigureProvider;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.datasource.DataSourceDescriptor;
import org.unidal.helper.Codes;

/**
 * @author chen.zhu
 * <p>
 * May 30, 2019
 */

public class XPipeDataSourceConfigureProvider implements IDataSourceConfigureProvider {

    private static final Logger logger = LoggerFactory.getLogger(XPipeDataSourceConfigureProvider.class);

    private DataSourceDescriptor descriptor;

    public XPipeDataSourceConfigureProvider(DataSourceDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public IDataSourceConfigure getDataSourceConfigure() {
        checkDataSourceDescriptor();
        return new XPipeDataSourceConfigure();
    }

    @Override
    public IDataSourceConfigure forceLoadDataSourceConfigure() {
        checkDataSourceDescriptor();
        return new XPipeDataSourceConfigure();
    }

    private void checkDataSourceDescriptor() {
        if(descriptor == null) {
            throw new XPipeDataSourceDescNotFoundException("unidal descriptor is null");
        }
        if(descriptor.getProperty("user", "").isEmpty()) {
            throw new XPipeDataSourceDescNotFoundException("database user is null");
        }
        if(descriptor.getProperty("password", "").isEmpty()) {
            throw new XPipeDataSourceDescNotFoundException("database password is null");
        }
        if(descriptor.getProperty("url", "").isEmpty()) {
            throw new XPipeDataSourceDescNotFoundException("database url is null");
        }
    }

    public class XPipeDataSourceConfigure extends AbstractDataSourceConfigure {

        @Override
        public String getUserName() {
            return descriptor.getProperty("user", "root");
        }

        @Override
        public String getPassword() {
            return decode(descriptor.getProperty("password", "password"));
        }

        @Override
        public String getConnectionUrl() {
            return descriptor.getProperty("url", "");
        }

        @Override
        public String getDriverClass() {
            return descriptor.getProperty("driver", "mysql");
        }


        // lao wu's tricky decode for password, even if I got the password from Apollo insteadof unidal itself
        private String decode(String src) {
            if (src == null) {
                return null;
            }

            if (src.startsWith("~{") && src.endsWith("}")) {
                try {
                    return Codes.forDecode().decode(src.substring(2, src.length() - 1));
                } catch (Exception e) {
                    logger.error("Unable to decode value: {}", src, e);
                }
            }

            return src;
        }
    }
}
