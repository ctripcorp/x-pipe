package com.ctrip.xpipe.redis.console.ds;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.codehaus.plexus.logging.LogEnabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceDescriptor;
import org.unidal.dal.jdbc.datasource.DataSourceException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Named;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author chen.zhu
 * <p>
 * May 30, 2019
 */

@Named(type = DataSource.class, value = "xpipe", instantiationStrategy = Named.PER_LOOKUP)
public class XPipeDataSource extends ContainerHolder implements DataSource, LogEnabled {

    private static final Logger logger = LoggerFactory.getLogger(XPipeDataSource.class);

    private org.codehaus.plexus.logging.Logger m_logger;

    private static final String ctripDalDataSource =
            "com.ctrip.xpipe.service.datasource.CtripDalBasedDataSource";

    private static boolean ctripDataSourceEnabled =
            ClassUtils.isPresent(ctripDalDataSource, XPipeDataSource.class.getClassLoader());

    private DataSource m_delegate;

    @Override
    public Connection getConnection() throws SQLException {
        return m_delegate.getConnection();
    }

    @Override
    public DataSourceDescriptor getDescriptor() {
        return m_delegate.getDescriptor();
    }

    @Override
    public void initialize(DataSourceDescriptor descriptor) {
        if (ctripDataSourceEnabled) {
            try {
                m_delegate = (DataSource)(Class.forName(ctripDalDataSource).newInstance());
            } catch (Throwable ex) {
                logger.error("Loading ctrip datasource failed", ex);
            }
        }
        if (m_delegate == null) {
            try {
                m_delegate = getContainer().lookup(DataSource.class, "jdbc");
            } catch (ComponentLookupException e) {
                throw new DataSourceException("unidal jdbc datasource not found");
            }
        }
        if (m_delegate instanceof LogEnabled) {
            ((LogEnabled)m_delegate).enableLogging(m_logger);
        }
        m_delegate.initialize(descriptor);

        logger.info("[initialize][DataSource]{}", m_delegate);
    }

    @Override
    public void enableLogging(org.codehaus.plexus.logging.Logger logger) {
        m_logger = logger;
    }
}
