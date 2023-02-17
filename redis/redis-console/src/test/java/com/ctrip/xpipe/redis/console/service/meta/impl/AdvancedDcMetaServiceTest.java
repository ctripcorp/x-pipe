package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ProxyTbl;
import com.ctrip.xpipe.redis.console.model.RouteTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.ctrip.xpipe.redis.console.service.RouteService;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.retry.RetryDelay;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chen.zhu
 * <p>
 * Apr 02, 2018
 */
public class AdvancedDcMetaServiceTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private DcMetaService dcMetaService;

    @Autowired
    private AdvancedDcMetaService service;

    @Autowired
    private ProxyService proxyService;

    @Autowired
    private RouteService routeService;

    @Autowired
    private ClusterService clusterService;

    @Test
    public void testRetry3TimesUntilSuccess() throws Exception {
        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
        service.setScheduled(scheduled).setFactory(new DefaultRetryCommandFactory(3, new RetryDelay(10), scheduled));
        Command<String> command = service.retry3TimesUntilSuccess(new AbstractCommand<String>() {

            private AtomicInteger counter = new AtomicInteger(0);

            @Override
            protected void doExecute() throws Exception {
                int currentCount = counter.getAndIncrement();
                logger.info(String.format("Run %d time", currentCount));
                if(currentCount > 1) {
                    future().setSuccess("success");
                } else {
                    throw new XpipeRuntimeException("test exception");
                }
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return "test-retry";
            }
        });

        AtomicBoolean complete = new AtomicBoolean(false);
        command.future().addListener(commandFuture -> {
            Assert.assertEquals("success", commandFuture.getNow());
            complete.getAndSet(true);
        });

        command.execute();

        waitConditionUntilTimeOut(() -> complete.get());
    }

    @Test
    public void testGetDcMeta() throws Exception {
        long start = System.currentTimeMillis();
        DcMeta dcMeta = service.getDcMeta("jq");
        long end = System.currentTimeMillis();
        logger.info("[durationMilli] {}", end - start);
        logger.info("[]{}", dcMeta);
    }

    @Test
    public void testGetDcMeta2() throws Exception {
        long start = System.currentTimeMillis();
        for(int i = 0; i < 100; i++) {
            dcMetaService.getDcMeta(dcNames[(1&i)]);
        }
        long end = System.currentTimeMillis();
        logger.info("[durationMilli] {}", (end - start)/100);
    }

    @Test
    public void testGetAllRouteCommand() throws InterruptedException {
        DcMeta meta = new DcMeta().setId("fra");
        (service.new GetAllRouteCommand(meta))
                .execute();
        logger.info("{}", meta);
    }

    @Test
    public void testProcess() {
        DcMeta meta = new DcMeta().setId("jq");
        List<RouteTbl> routes = routeService.getActiveRouteTbls();
        List<ProxyTbl> proxies = proxyService.getActiveProxyTbls();
        List<RouteMeta> routeMetas = service.combineRouteInfo(routes, proxies, meta);
        routeMetas.forEach((routeMeta)->meta.addRoute(routeMeta));
        logger.info("{}", meta);
    }

    @Test
    public void testClusterOrgInfo() throws Exception {
        List<ClusterTbl> clusterTbls = clusterService.findAllClustersWithOrgInfo();

        ClusterTbl clusterTbl = clusterTbls.get(3);
        clusterTbl.setClusterOrgId(3);
        clusterService.update(clusterTbl);

        DcMeta dcMeta = service.getDcMeta("jq");

        ClusterMeta clusterMeta = dcMeta.findCluster(clusterTbl.getClusterName());
        Assert.assertNotNull(clusterMeta.getOrgId());
        Assert.assertTrue(3 == clusterMeta.getOrgId());
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/apptest.sql");
    }

    @Ignore
    @Test
    public void testHangForever() throws Exception {
//        proxyService.deleteProxy();
        dcMetaService.getDcMeta("jq");
    }
}