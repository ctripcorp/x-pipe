package com.ctrip.xpipe.redis.console.dal;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.KeepercontainerService;
import com.ctrip.xpipe.redis.console.service.SentinelService;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author chen.zhu
 * <p>
 * Nov 08, 2017
 */

public class DataObjectAssemblyTest extends AbstractConsoleIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(DataObjectAssemblyTest.class);

    @Autowired
    DcMetaService service;

    @Autowired
    DcService dcService;

    @Autowired
    MetaCache metaCache;

    @Autowired
    SentinelService sentinelService;

    @Autowired
    KeepercontainerService keepercontainerService;

    @Resource(name = ConsoleContextConfig.GLOBAL_EXECUTOR)
    private ExecutorService executor;

    private String dcName;

    private int N;

    private static final String SQL_PATH = "src/test/resources/DataObjectAssembly-Test.sql";

    @Before
    public void beforeDataObjectAssemblyTest() throws Exception {
        dcName = dcService.dcNameMap().values().iterator().next();
        N = 20;
        startH2Server();
    }

    @Test
    public void testGetDataNotNull() throws InterruptedException, ExecutionException {
        CyclicBarrier barrier = new CyclicBarrier(N);
        List<Future<List<DcTbl>>> futures = new LinkedList<>();
        for(int i = 0; i < N; i ++) {
            futures.add(getDCDetails(barrier));
        }

        for(Future<List<DcTbl>> future : futures) {
            List<DcTbl> details = future.get();
            for(DcTbl dc : details) {
                Assert.assertNotNull(dc);
                Assert.assertNotNull(dc.getClusterInfo());
                Assert.assertNotNull(dc.getClusterInfo().getClusterName());
                Assert.assertNotNull(dc.getClusterInfo().getStatus());
                Assert.assertNotNull(dc.getClusterInfo().getDataChangeLastTime());
                Assert.assertNotNull(dc.getDcClusterShardInfo());
                Assert.assertNotNull(dc.getDcClusterInfo());
                Assert.assertNotNull(dc.getDcClusterInfo().getDataChangeLastTime());
                Assert.assertNotNull(dc.getRedisInfo());
                Assert.assertNotNull(dc.getRedisInfo().getRunId());
                Assert.assertNotNull(dc.getRedisInfo().getRedisRole());
                Assert.assertNotNull(dc.getRedisInfo().getRedisIp());
                Assert.assertNotNull(dc.getShardInfo());
                Assert.assertNotNull(dc.getShardInfo().getShardName());
            }
        }
    }


    private Future<List<DcTbl>> getDCDetails(CyclicBarrier barrier) {
        Future<List<DcTbl>> future_allDetails = executor.submit(new Callable<List<DcTbl>>() {
            @Override
            public List<DcTbl> call() throws Exception {
                barrier.await();
                return dcService.findAllDetails(dcName);
            }
        });
        return future_allDetails;
    }


    @Override
    public String prepareDatas() throws IOException {
        return prepareDatasFromFile(SQL_PATH);
    }

    @After
    public void afterDataObjectAssemblyTest() {

    }
}
