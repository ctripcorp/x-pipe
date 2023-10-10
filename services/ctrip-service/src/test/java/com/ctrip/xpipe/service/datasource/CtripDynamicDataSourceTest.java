package com.ctrip.xpipe.service.datasource;

import org.apache.tomcat.jdbc.pool.PoolExhaustedException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;

/**
 * @author lishanglin
 * date 2021/4/13
 */
@Ignore
public class CtripDynamicDataSourceTest {

    // 1、start mysqld
    // 2、prepare local config /opt/config/100004374/qconfig/datasource.properties,
    // /opt/config/100004374/dal/local-databases.properties, /opt/config/100004374/qconfig/application.properties

    private CtripDynamicDataSource dataSource;

    @Before
    public void setupCtripDynamicDataSourceTest() throws Exception {
        this.dataSource = new CtripDynamicDataSource(new CtripDalDataSourceFactory());
        this.dataSource.initialize(null);
    }

    @Test
    public void testAccessLocalMySQL() throws Exception {
        Connection connection = dataSource.getConnection();
        connection.close();
    }

    // set maxActive=1, maxIdle=1 in local datasource.properties
    @Test(expected = PoolExhaustedException.class)
    public void testMaxActive1() throws Exception {
        dataSource.getConnection();
        dataSource.getConnection();
    }

}
