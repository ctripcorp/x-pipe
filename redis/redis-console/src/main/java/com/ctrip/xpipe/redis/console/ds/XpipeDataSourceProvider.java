package com.ctrip.xpipe.redis.console.ds;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;
import org.unidal.dal.jdbc.datasource.DataSourceProvider;
import org.unidal.dal.jdbc.datasource.DefaultDataSourceProvider;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourcesDef;
import org.unidal.lookup.annotation.Named;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Named(type = DataSourceProvider.class, value = "xpipe")
public class XpipeDataSourceProvider implements DataSourceProvider, LogEnabled, Initializable {
    private Logger m_logger;
    private String m_datasourceFile;
    private String m_baseDirRef;
    private String m_defaultBaseDir;
    private DataSourceProvider m_delegate;

    private org.slf4j.Logger logger = LoggerFactory.getLogger(getClass());

    private static final String qconfigDataSourceProviderClass =
            "com.ctrip.xpipe.service.datasource.QConfigDataSourceProvider";
    private static boolean qconfigDataSourceProviderPresent =
            ClassUtils.isPresent(qconfigDataSourceProviderClass, XpipeDataSourceProvider.class.getClassLoader());

    @Override
    public void initialize() throws InitializationException {
        if (qconfigDataSourceProviderPresent) {
            try {
                m_delegate = (DataSourceProvider)(Class.forName(qconfigDataSourceProviderClass).newInstance());
            } catch (Throwable ex) {
                m_logger.error("Loading qconfig datasource provider failed", ex);
            }
        }
        if (m_delegate == null) {
            m_delegate = createDefaultDataSourceProvider();
        }
        if (m_delegate instanceof LogEnabled) {
            ((LogEnabled)m_delegate).enableLogging(m_logger);
        }
        logger.info("[initialize][DataSourceProvider]{}", m_delegate);
    }

    private DefaultDataSourceProvider createDefaultDataSourceProvider() {
        DefaultDataSourceProvider ds = new DefaultDataSourceProvider();
        ds.setBaseDirRef(m_baseDirRef);
        ds.setDatasourceFile(m_datasourceFile);
        ds.setDefaultBaseDir(m_defaultBaseDir);
        return ds;
    }

    @Override
    public void enableLogging(Logger logger) {
        m_logger = logger;
    }

    @Override
    public DataSourcesDef defineDatasources() {
        return m_delegate.defineDatasources();
    }

    public void setBaseDirRef(String baseDirRef) {
        this.m_baseDirRef = baseDirRef;
        this.reInit();
    }

    public void setDatasourceFile(String datasourceFile) {
        this.m_datasourceFile = datasourceFile;
        this.reInit();
    }

    public void setDefaultBaseDir(String defaultBaseDir) {
        this.m_defaultBaseDir = defaultBaseDir;
        this.reInit();
    }

    private void reInit() {
        // only for test
        m_delegate = null;
        try {
            this.initialize();
        } catch (Throwable ex) {
            logger.error("[reInit]" + ex);
        }
    }
}
