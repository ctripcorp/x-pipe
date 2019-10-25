package com.ctrip.xpipe.service.datasource;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.util.ClassUtils;

public class CtripDalBasedDataSourceTest {


    @Test
    public void testConstruction() {
       final String ctripDalDataSource =
               "com.ctrip.xpipe.service.datasource.CtripDalBasedDataSource";

       boolean ctripDataSourceEnabled =
               ClassUtils.isPresent(ctripDalDataSource, Thread.currentThread().getContextClassLoader());

        Assert.assertTrue(ctripDataSourceEnabled);

   }

   @Test
   public void testDataSource() {
        CtripDalBasedDataSource dataSource = new CtripDalBasedDataSource();
   }
}