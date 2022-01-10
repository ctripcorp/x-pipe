package com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationResponse;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.spring.WebClientFactory;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * @author lishanglin
 * date 2021/10/21
 */
public class ConsoleContinueMigrationTest extends AbstractConsoleIntegrationTest {

    private int qps = 30;

    private AtomicInteger continueTimes = new AtomicInteger(0);

    private ScheduledExecutorService scheduled;

    private Queue<String> waitMigrationClusters = new ConcurrentLinkedQueue<>();

    private WebClient client;

    private String sourceDc = "PTOY";

    private String targetDc = "PTJQ";

    private String console = "http://10.2.39.18:8080";

    @Autowired
    private ClusterService clusterService;

    @Before
    public void setupContinueMigrationTest() throws Exception {
        scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create(getTestName()));
        client = WebClientFactory.makeWebClient(3000, 30000,
                loopResources, ConnectionProvider.builder("TestConnProvider").maxConnections(1000)
                .pendingAcquireTimeout(Duration.ofMillis(1000)).maxIdleTime(Duration.ofMillis(10000)).build());

        initClustersFromDb();
    }

    @Test
    public void testContinueMigration() throws Exception {
        scheduled.scheduleWithFixedDelay(this::migrateMultiClusters, 0, 1, TimeUnit.SECONDS);
        waitConditionUntilTimeOut(waitMigrationClusters::isEmpty, 2400000, 10000);
    }

    @Test
    public void testContinueMigrationToAndBack() throws Exception {
        this.continueTimes.set(10);
        scheduled.scheduleWithFixedDelay(this::migrateMultiClusters, 0, 1, TimeUnit.SECONDS);
        waitConditionUntilTimeOut(() -> 0 == continueTimes.get(), 2400000, 10000);
    }

    private void switchTargetDc() {
        String tmpTarget = this.targetDc;
        this.targetDc = this.sourceDc;
        this.sourceDc = tmpTarget;
    }

    private void initClustersFromDb() {
        List<ClusterTbl> clusterTbls = clusterService.findActiveClustersByDcName(sourceDc);
        clusterTbls.forEach(clusterTbl -> waitMigrationClusters.add(clusterTbl.getClusterName()));
    }

    private void initClusters() {
        IntStream.rangeClosed(0, 20000).forEach(i -> {
            String cluster = "cluster" + i;
            waitMigrationClusters.add(cluster);
        });
    }

    private void migrateMultiClusters() {
        for (int i = 0; i < qps; i++) {
            if (!migrateNextCluster()) break;
        }
        if (waitMigrationClusters.isEmpty() && continueTimes.get() > 0) {
            continueTimes.decrementAndGet();
            switchTargetDc();
            initClustersFromDb();
        }
    }

    private boolean migrateNextCluster() {
        String cluster = waitMigrationClusters.poll();
        if (null == cluster) {
            logger.info("[migrateNextCluster] clusters clear");
            return false;
        }

        reqMigration(cluster, targetDc).addListener(migFuture -> {
            if (migFuture.isSuccess()) {
                logger.info("[migrateNextCluster][{}] success {}", cluster, migFuture.get().getMsg());
            } else {
                logger.info("[migrateNextCluster][{}] fail", cluster, migFuture.cause());
            }
        });

        return true;
    }

    private CommandFuture<BeaconMigrationResponse> reqMigration(String cluster, String targetDc) {
        CommandFuture<BeaconMigrationResponse> future = new DefaultCommandFuture<>();

        BeaconMigrationRequest request = new BeaconMigrationRequest();
        request.setClusterName(cluster);
        request.setIsForced(true);
        request.setTargetIDC(targetDc);
        request.setGroups(Collections.emptySet());

        logger.info("[reqMigration][{}] at {}", cluster, System.currentTimeMillis());
        client.post()
                .uri(console + "/api/beacon/migration/sync")
                .bodyValue(request)
                .retrieve()
                .bodyToMono(BeaconMigrationResponse.class)
                .subscribe(future::setSuccess, future::setFailure);

        return future;
    }

    @Override
    protected boolean resetDbData() {
        return false;
    }

    @After
    public void afterContinueMigrationTest() {
        scheduled.shutdownNow();
    }

}
