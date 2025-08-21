package com.ctrip.xpipe.redis.console.dal;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.DcService;
import jakarta.annotation.Resource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

/**
 * @author chen.zhu
 * <p>
 * Nov 08, 2017
 */

public class DataObjectAssemblyTest extends AbstractConsoleIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(DataObjectAssemblyTest.class);

    @Autowired
    private DcService dcService;

    @Autowired
    private DcClusterShardService dcClusterShardService;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executor;

    private long dcId;

    private int N;

    private static final String SQL_PATH = "src/test/resources/DataObjectAssembly-Test.sql";

    @Before
    public void beforeDataObjectAssemblyTest() throws Exception {
        dcId = dcService.dcNameMap().keySet().iterator().next();
        N = 20;
        startH2Server();
    }

    @Test
    public void testGetDataNotNull() throws InterruptedException, ExecutionException {
        CyclicBarrier barrier = new CyclicBarrier(N);
        List<Future<List<DcClusterShardTbl>>> futures = new LinkedList<>();
        for(int i = 0; i < N; i ++) {
            futures.add(getDCDetails(barrier));
        }

        for(Future<List<DcClusterShardTbl>> future : futures) {
            List<DcClusterShardTbl> details = future.get();
            for(DcClusterShardTbl item : details) {
                Assert.assertNotNull(item);
                Assert.assertNotNull(item.getClusterInfo());
                Assert.assertNotNull(item.getClusterInfo().getClusterName());
                Assert.assertNotNull(item.getClusterInfo().getStatus());
                Assert.assertNotNull(item.getRedisInfo());
                Assert.assertNotNull(item.getRedisInfo().getRunId());
                Assert.assertNotNull(item.getRedisInfo().getRedisRole());
                Assert.assertNotNull(item.getRedisInfo().getRedisIp());
                Assert.assertNotNull(item.getShardInfo());
                Assert.assertNotNull(item.getShardInfo().getShardName());
            }
        }
    }


    private Future<List<DcClusterShardTbl>> getDCDetails(CyclicBarrier barrier) {
        Future<List<DcClusterShardTbl>> future_allDetails = executor.submit(new Callable<List<DcClusterShardTbl>>() {
            @Override
            public List<DcClusterShardTbl> call() throws Exception {
                barrier.await();
                return dcClusterShardService.findAllByDcId(dcId);
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
