package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ApplierTbl;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;

public class ApplierDaoTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ApplierDao applierDao;

    @Test
    public void testFindByDcCLusterShard() {
        List<ApplierTbl> appliers = applierDao.findByDcClusterShard(51L);
        Assert.assertEquals(2, appliers.size());
    }

    @Override
    protected String prepareDatas() throws IOException {
        return  prepareDatasFromFile("src/test/resources/hetero-cluster-test.sql");
    }
}