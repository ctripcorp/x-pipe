package com.ctrip.xpipe.service.datasource;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;
import qunar.tc.qconfig.client.TypedConfig;

import static org.junit.Assert.*;

/**
 * @author zhuchen
 * Apr 2020/4/8 2020
 */
public class QConfigDataSourceProviderTest extends AbstractTest {

    @Test
    public void testQConfigDataSource() {
        TypedConfig<String> dataSourceConfig = TypedConfig.get("datasources.xml", new QConfigDataSourceProvider.CustomParser());
        logger.info("{}", dataSourceConfig.current());
    }

    @Test
    public void testApolloDataSource() {
        ConfigFile file = ConfigService.getConfigFile("datasources", ConfigFileFormat.XML);

        if (file != null && file.hasContent()) {
            String content = file.getContent();
            logger.info("{}", content);
        }
    }

    @Test
    public void diff() {
        ConfigFile file = ConfigService.getConfigFile("datasources", ConfigFileFormat.XML);

        String content = file.getContent();
        logger.info("{}", content);

        TypedConfig<String> dataSourceConfig = TypedConfig.get("datasources.xml", new QConfigDataSourceProvider.CustomParser());
        logger.info("{}", dataSourceConfig.current());

        Assert.assertEquals(file.getContent(), dataSourceConfig.current());
    }
}