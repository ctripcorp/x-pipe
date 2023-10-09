package com.ctrip.xpipe.service.datasource;

import org.junit.Assert;
import org.junit.Test;

import javax.sql.DataSource;

public class CtripDalDataSourceFactoryTest {

    @Test
    public void getSameDataSource() throws Exception {
        CtripDalDataSourceFactory factory = new CtripDalDataSourceFactory();
        DataSource dataSource1 = factory.getOrCreateDataSource();
        DataSource dataSource2 = factory.getOrCreateDataSource();
        Assert.assertEquals(dataSource1, dataSource2);
    }
}
