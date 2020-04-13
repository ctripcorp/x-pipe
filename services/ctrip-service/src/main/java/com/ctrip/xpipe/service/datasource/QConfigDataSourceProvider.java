package com.ctrip.xpipe.service.datasource;

import com.ctrip.framework.foundation.Foundation;
import com.ctrip.xpipe.utils.StringUtil;
import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.unidal.dal.jdbc.datasource.DataSourceProvider;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourcesDef;
import org.unidal.dal.jdbc.datasource.model.transform.DefaultSaxParser;
import org.unidal.lookup.annotation.Named;
import qunar.tc.qconfig.client.TypedConfig;

import java.io.IOException;

/**
 * @author zhuchen
 * Apr 2020/4/8 2020
 */
@Named(type = DataSourceProvider.class, value = "qconfig")
public class QConfigDataSourceProvider implements DataSourceProvider, LogEnabled {
    private Logger m_logger;

    private DataSourcesDef m_def;

    @Override
    public DataSourcesDef defineDatasources() {
        if (m_def == null) {
            TypedConfig<String> dataSourceConfig = TypedConfig.get("datasources.xml", new CustomParser());
            String appId = Foundation.app().getAppId();

            String envType = Foundation.server().getEnvFamily().getName();

            if (!StringUtil.isEmpty(dataSourceConfig.current())) {
                String content = dataSourceConfig.current();

                m_logger.info(String.format("Found datasources.xml from QConfig(env=%s, app.id=%s)!", envType, appId));

                try {
                    m_def = DefaultSaxParser.parse(content);
                } catch (Exception e) {
                    throw new IllegalStateException(String.format("Error when parsing datasources.xml from QConfig(env=%s, app.id=%s)!", envType, appId), e);
                }
            } else {
                m_logger.warn(String.format("Can't get datasources.xml from QConfig(env=%s, app.id=%s)!", envType, appId));
                m_def = new DataSourcesDef();
            }
        }

        return m_def;
    }

    @Override
    public void enableLogging(Logger logger) {
        m_logger = logger;
    }

    public static class CustomParser implements TypedConfig.Parser<String> {

        @Override
        public String parse(String data) throws IOException {
            return data;
        }
    }
}
