package com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.spring.WebClientFactory;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lishanglin
 * date 2021/12/24
 */
public class MetaserverContinueMigrationTest extends AbstractConsoleIntegrationTest {

    private int migIntervalMill = 100;

    private int migEveryRound = 30;

    private int needMigTimes = 20;

    private String sourceDc = "PTJQ";

    private String[] otherDcs = new String[] {"PTOY"};

    private String metaserver = "http://10.0.0.1:8080";

    private String migrateUri;

    private WebClient client;

    private Queue<ContinueMigrationClusterShard> waitMigrationClusterShards = new ConcurrentLinkedQueue<>();

    @Before
    public void beforeMetaserverContinueMigrationTest() {
        this.migrateUri = META_SERVER_SERVICE.CHANGE_PRIMARY_DC.getRealPath(metaserver);
        initClusterShardsFromDb();
        client = WebClientFactory.makeWebClient(3000, 30000,
                loopResources, ConnectionProvider.builder("TestConnProvider").maxConnections(1000)
                        .pendingAcquireTimeout(Duration.ofMillis(1000)).maxIdleTime(Duration.ofMillis(10000)).build());
    }

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ShardService shardService;

    private void initClusterShardsFromDb() {
        List<ClusterTbl> clusterTbls = clusterService.findAllClusterByDcNameBind(sourceDc);
        String[] targetDcs = buildTargetDcs();
        for (ClusterTbl clusterTbl: clusterTbls) {
            List<ShardTbl> shardTbls = shardService.findAllByClusterName(clusterTbl.getClusterName());
            for (ShardTbl shardTbl: shardTbls) {
                waitMigrationClusterShards.add(new ContinueMigrationClusterShard(clusterTbl.getClusterName(), shardTbl.getShardName(), needMigTimes, targetDcs));
            }
        }
    }

    @Test
    public void testContinueMigrate() throws Exception {
        ScheduledFuture<?> scheduledFuture = null;
        try {
            scheduledFuture = scheduled.scheduleWithFixedDelay(this::doMigration, 0, migIntervalMill, TimeUnit.MILLISECONDS);
            waitConditionUntilTimeOut(waitMigrationClusterShards::isEmpty, 600000, 10000);
        } finally {
            if (null != scheduledFuture && !scheduledFuture.isDone()) {
                scheduledFuture.cancel(true);
            }
        }
    }

    @Test
    public void testContinueMigrateWithoutInterval() throws Exception {
        while (!waitMigrationClusterShards.isEmpty()) {
            this.doMigration();
        }
        logger.info("[testContinueMigrateWithoutInterval][finish] {}", System.currentTimeMillis());
    }

    private void doMigration() {
        waitMigrationClusterShards.removeIf(ContinueMigrationClusterShard::isMigFinished);
        logger.info("[doMigration][begin] {}, wait cluster {}", System.currentTimeMillis(), waitMigrationClusterShards.size());
        waitMigrationClusterShards.stream()
                .filter(ContinueMigrationClusterShard::isIdeal).limit(migEveryRound)
                .forEach(clusterShard -> {
                    try {
                        logger.info("[doMigration][exec] {}", clusterShard);
                        clusterShard.doMigrate(client);
                    } catch (Throwable th) {
                        logger.info("[doMigration][{}] start mig fail", clusterShard, th);
                    }
                });
    }

    private String[] buildTargetDcs() {
        String[] targetDcs = new String[otherDcs.length + 1];
        System.arraycopy(otherDcs, 0, targetDcs, 0, otherDcs.length);
        targetDcs[otherDcs.length] = sourceDc;
        return targetDcs;
    }

    @Override
    protected boolean resetDbData() {
        return false;
    }

    private class ContinueMigrationClusterShard {

        private final int migTimesBeforeStop;

        private final String cluster;

        private final String shard;

        private final String[] targetDcs;

        private AtomicInteger migTimes;

        private AtomicBoolean onMigrating;

        public ContinueMigrationClusterShard(String cluster, String shard, int totalMigTimes, String[] targetDcs) {
            this.migTimesBeforeStop = totalMigTimes;
            this.cluster = cluster;
            this.shard = shard;
            this.targetDcs = targetDcs;
            this.migTimes = new AtomicInteger();
            this.onMigrating = new AtomicBoolean(false);
        }

        private String getTargetDc() {
            return targetDcs[this.migTimes.get() % targetDcs.length];
        }

        private void migSuccess(MetaServerConsoleService.PreviousPrimaryDcMessage resp) {
            logger.info("[{}][{}][{}][success]", cluster, shard, getTargetDc());
            this.onMigrating.set(false);
            this.migTimes.incrementAndGet();
        }

        private void migFail(Throwable th) {
            this.onMigrating.set(false);
            logger.info("[{}][{}][{}][fail]", cluster, shard, getTargetDc());
        }

        public void doMigrate(WebClient client) {
            this.onMigrating.set(true);
            String targetDc = getTargetDc();
            client.put()
                    .uri(migrateUri, cluster, shard, targetDc)
                    .header("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .bodyToMono(MetaServerConsoleService.PreviousPrimaryDcMessage.class)
                    .subscribe(this::migSuccess, this::migFail);
        }

        public boolean isIdeal() {
            return this.migTimes.get() < this.migTimesBeforeStop && !this.onMigrating.get();
        }

        public boolean isMigFinished() {
            return !this.onMigrating.get() && this.migTimes.get() >= this.migTimesBeforeStop;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ContinueMigrationClusterShard that = (ContinueMigrationClusterShard) o;
            return Objects.equals(cluster, that.cluster) &&
                    Objects.equals(shard, that.shard);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cluster, shard);
        }

        @Override
        public String toString() {
            return String.format("Mig[%s,%s]", cluster, shard);
        }
    }

}
