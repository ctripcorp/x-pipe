package com.ctrip.xpipe.service.datasource;

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
}