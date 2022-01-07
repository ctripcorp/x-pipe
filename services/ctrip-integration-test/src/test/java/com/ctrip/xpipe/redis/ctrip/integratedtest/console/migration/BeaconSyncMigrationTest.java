package com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration;

import com.ctrip.xpipe.api.sso.SsoConfig;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationResponse;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.ctrip.integratedtest.console.AbstractCtripConsoleIntegrationTest;
import com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration.mock.MockMigrationEventDao;
import com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration.mock.MockMysqlReadHandler;
import com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration.mock.MockMysqlWriteHandler;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Sets;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.PropertyValues;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.InstantiationAwareBeanPostProcessor;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.*;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;
import org.unidal.dal.jdbc.query.DefaultQueryExecutor;
import org.unidal.dal.jdbc.query.QueryExecutor;
import org.unidal.dal.jdbc.query.ReadHandler;
import org.unidal.dal.jdbc.query.WriteHandler;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * @author lishanglin
 * date 2021/3/29
 */
@SpringBootApplication(scanBasePackages = "com.ctrip.xpipe.redis.console")
public class BeaconSyncMigrationTest extends AbstractCtripConsoleIntegrationTest {

    private static boolean onTest = false;

    private static boolean catDev = false;

    private static int readDelay = 0;

    private static int writeDelay = 0;

    private ConfigurableApplicationContext applicationContext;

    private RestOperations restOperations;

    private ExecutorService migrationExecutors;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcService dcService;

    @Autowired
    private RedisService redisService;

    @Autowired
    private ConfigService configService;

    private String srcDc = "jq";

    private String targetDc = "oy";

    private Map<String, String> redisForDcs = new HashMap<String, String>() {{
        put(srcDc, "10.0.0.1");
        put(targetDc, "10.0.0.2");
    }};

    private int shardsPerCluster = 5;

    private int redisesPerShard = 2;

    private int serverPort;

    @Before
    public void setupBeaconSyncMigrationTest() throws Exception {
        restOperations = RestTemplateFactory.createCommonsHttpRestTemplate(1000, 1000, 1000, 15000);

        onTest = true;
        SsoConfig.stopsso = true;

        migrationExecutors = Executors.newFixedThreadPool(1000, XpipeThreadFactory.create("BeaconSyncMigrationTest"));

        serverPort = randomPort();
        logger.info("[setupBeaconSyncMigrationTest] console start on port {}", serverPort);

        applicationContext = new SpringApplicationBuilder(BeaconSyncMigrationTest.class).run(
                "--server.tomcat.max-threads=1", "--server.port=" + serverPort, "--cat.dev.mode="+catDev);
        serverPort = Integer.parseInt(applicationContext.getEnvironment().getProperty("server.port"));
        waitConditionUntilTimeOut(this::checkConsoleHealth, 10000, 1000);

        configService.doIgnoreMigrationSystemAvailability(true);
    }

    @After
    public void afterBeaconSyncMigrationTest() throws Exception {
        onTest = false;
        SsoConfig.stopsso = false;
        readDelay = 0;
        writeDelay = 0;
        if (applicationContext != null) {
            applicationContext.stop();
        }
    }

    @Test
    @Ignore
    public void startAsConsole() throws Exception {
        fillInCluster(10);
        waitForAnyKeyToExit();
    }

    @Test
    // make sure migration-threads-cnt ge concurrentCnt
    public void testConcurrentMigrationNoBlockTomcat() throws Exception {
        int concurrentCnt = 4;
        CountDownLatch latch = new CountDownLatch(concurrentCnt);
        AtomicInteger successCnt = new AtomicInteger(0);
        fillInCluster(concurrentCnt);

        IntStream.range(0, concurrentCnt).forEach(i -> {
            executors.submit(() -> {
                BeaconMigrationRequest request = new BeaconMigrationRequest();
                request.setClusterName("cluster" + i);
                request.setIsForced(true);
                request.setTargetIDC("oy");
                request.setGroups(Collections.emptySet());
                try {
                    BeaconMigrationResponse response = restOperations.postForObject(String.format("http://127.0.0.1:%d/api/beacon/migration/sync", serverPort),
                            request, BeaconMigrationResponse.class);
                    logger.info("[testConcurrentMigrationNoBlockTomcat] {}", response);
                    if (0 == response.getCode()) successCnt.incrementAndGet();
                } catch (Exception e) {
                    logger.info("[testConcurrentMigrationNoBlockTomcat] fail", e);
                } finally {
                    latch.countDown();
                }
            });
        });

        latch.await(11, TimeUnit.SECONDS);
        waitConditionUntilTimeOut(() -> concurrentCnt == successCnt.get(), 100000);
        Assert.assertEquals(concurrentCnt, successCnt.get());
    }

    @Test
    public void testOneDBConnectMigration() {
        fillInCluster(1);

        BeaconMigrationRequest request = new BeaconMigrationRequest();
        request.setClusterName("cluster0");
        request.setIsForced(true);
        request.setTargetIDC("oy");
        request.setGroups(Collections.emptySet());
        BeaconMigrationResponse response = restOperations.postForObject(String.format("http://127.0.0.1:%d/api/beacon/migration/sync", serverPort),
                request, BeaconMigrationResponse.class);

        logger.info("[testOneDBConnectMigration] resp {}", response);
        Assert.assertEquals(0, response.getCode());
    }

    @Test
    public void testSlowDb() throws Throwable {
        readDelay = 100;
        writeDelay = 500;
        int concurrentCnt = 4;
        CountDownLatch latch = new CountDownLatch(concurrentCnt);
        fillInCluster(concurrentCnt);

        IntStream.range(0, concurrentCnt).forEach(i -> {
            executors.submit(() -> {
                BeaconMigrationRequest request = new BeaconMigrationRequest();
                request.setClusterName("cluster" + i);
                request.setIsForced(true);
                request.setTargetIDC("oy");
                request.setGroups(Collections.emptySet());
                try {
                    BeaconMigrationResponse response = restOperations.postForObject(String.format("http://127.0.0.1:%d/api/beacon/migration/sync", serverPort),
                            request, BeaconMigrationResponse.class);
                    logger.info("[testConcurrentMigrationNoBlockTomcat] {}", response);
                } catch (Exception e) {
                    logger.info("[testConcurrentMigrationNoBlockTomcat] fail", e);
                } finally {
                    latch.countDown();
                }
            });
        });

        waitConditionUntilTimeOut(() -> {
            AtomicBoolean success = new AtomicBoolean(true);
            IntStream.range(0, concurrentCnt).forEach(i -> {
                ClusterTbl clusterTbl = clusterService.find("cluster" + i);
                success.set(success.get() & clusterTbl.getActivedcId() == 2);
            });
            return success.get();
        }, 100000);
    }

    public void fillInCluster(int clusterCnt) {
        fillInCluster(0, clusterCnt);
    }

    public void fillInCluster(int startIndex, int clusterCnt) {
        List<DcTbl> dcs = dcService.findAllDcs();
        IntStream.range(startIndex, startIndex + clusterCnt).forEach(i -> {
            ClusterModel cluster = mockCluster(i, dcs);
            clusterService.createCluster(cluster);
            dcs.forEach(dc -> {
                String redisHost = redisForDcs.get(dc.getDcName());
                if (StringUtil.isEmpty(redisHost)) return;

                AtomicInteger startPort = new AtomicInteger(1000 + i*shardsPerCluster*redisesPerShard);
                cluster.getShards().forEach(shard -> {
                    try {
                        List<Pair<String, Integer>> redises = new ArrayList<>();
                        IntStream.range(startPort.get(), startPort.get()+redisesPerShard).forEach(redisPort -> redises.add(Pair.from(redisHost, redisPort)));
                        startPort.set(startPort.get() + redisesPerShard);
                        redisService.insertRedises(dc.getDcName(), cluster.getClusterTbl().getClusterName(),
                                shard.getShardTbl().getShardName(), redises);
                    } catch (Exception e) {
                        logger.warn("[fillInCluster][{}] add redis fail", i, e);
                    }
                });
            });
        });
    }

    private boolean checkConsoleHealth() {
        try {
            String rst = restOperations.getForObject(String.format("http://127.0.0.1:%d/api/redises/jq/cluster0/shard1", serverPort), String.class);
            logger.info("[checkConsoleHealth] rst {}", rst);
            return true;
        } catch (Exception e) {
            logger.info("[checkConsoleHealth] check fail", e);
            return false;
        }
    }

    @Test
    @Ignore
    public void concurrentMigration() {
        AtomicInteger rounds = new AtomicInteger(0);
        Set<String> successClusters = Sets.newConcurrentHashSet();

        do {
            long startIndex = rounds.get() * 1000;
            logger.info("[concurrentMigration] rounds {} start", rounds.get());
            IntStream.range(0, 1000).forEach(i -> {
                migrationExecutors.execute(() -> {
                    String cluster = "cluster" + ((startIndex + i) % 1000);
                    if (successClusters.contains(cluster)) return;

                    try {
                        BeaconMigrationRequest request = new BeaconMigrationRequest();
                        request.setIsForced(true);
                        request.setClusterName(cluster);
                        request.setTargetIDC(targetDc);
                        request.setGroups(Collections.emptySet());

                        logger.info("[concurrentMigration] migration start {}", cluster);
                        BeaconMigrationResponse response = restOperations.postForObject(String.format("http://127.0.0.1:%d/api/beacon/migration/sync", serverPort),
                                request, BeaconMigrationResponse.class);
                        if (0 != response.getCode()) {
                            logger.info("[concurrentMigration][{}] migration fail {}", cluster, response.getMsg());
                        } else {
                            logger.info("[concurrentMigration][{}] migration success {}", cluster, response.getMsg());
                            successClusters.add(cluster);
                        }
                    } catch (Exception e) {
                        logger.info("[concurrentMigration][{}] req fail", cluster, e);
                    }
                });
            });
            logger.info("[concurrentMigration] rounds {} end", rounds.getAndIncrement());
            sleep(10000);
        } while (!allClusterMigrationFinish());
    }

    private boolean allClusterMigrationFinish() {
        try {
            return clusterService.findActiveClustersByDcName(srcDc).isEmpty();
        } catch (Exception e) {
            logger.info("[allClusterMigrationFinish] sql fail", e);
            return false;
        }
    }

    private ClusterModel mockCluster(int index, List<DcTbl> dcs) {
        ClusterModel clusterModel = new ClusterModel();
        ClusterTbl clusterTbl = new ClusterTbl();
        clusterTbl.setClusterName("cluster" + index).setClusterType(ClusterType.ONE_WAY.name())
                .setClusterDescription("cluster" + index).setActivedcId(dcs.get(0).getId())
                .setClusterAdminEmails("test@trip.com").setClusterOrgId(0);

        clusterModel.setClusterTbl(clusterTbl);
        clusterModel.setDcs(dcs);

        List<ShardModel> shards = new ArrayList<>();
        IntStream.range(1, 1 + shardsPerCluster).forEach(i -> {
            ShardModel shardModel = new ShardModel();
            ShardTbl shardTbl = new ShardTbl();
            shardTbl.setShardName("shard" + i);
            shardModel.setShardTbl(shardTbl);

            shards.add(shardModel);
        });

        clusterModel.setShards(shards);
        return clusterModel;
    }

    @Component
    @Profile(AbstractProfile.PROFILE_NAME_TEST)
    public static class UnidalContainerMocker {

        @PostConstruct
        public void postConstruct() {
            if (!onTest) return;

            try {
                DefaultQueryExecutor queryExecutor = (DefaultQueryExecutor)ContainerLoader.getDefaultContainer().lookup(QueryExecutor.class);
                Field writeHandlerField = queryExecutor.getClass().getDeclaredField("m_writeHandler");
                Field readHandlerField = queryExecutor.getClass().getDeclaredField("m_readHandler");
                writeHandlerField.setAccessible(true);
                readHandlerField.setAccessible(true);

                MockMysqlWriteHandler mockMysqlWriteHandler = new MockMysqlWriteHandler((WriteHandler) writeHandlerField.get(queryExecutor), () -> writeDelay);
                MockMysqlReadHandler mockMysqlReadHandler = new MockMysqlReadHandler((ReadHandler) readHandlerField.get(queryExecutor), () -> readDelay);
                writeHandlerField.set(queryExecutor, mockMysqlWriteHandler);
                readHandlerField.set(queryExecutor, mockMysqlReadHandler);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Component
    @Profile(AbstractProfile.PROFILE_NAME_TEST)
    public static class MigrationTestBeanPostProcessor implements InstantiationAwareBeanPostProcessor {

        private Logger logger = LoggerFactory.getLogger(MigrationTestBeanPostProcessor.class);

        @Override
        public Object postProcessBeforeInstantiation(Class<?> beanClass, String beanName) throws BeansException {
            return null;
        }

        @Override
        public boolean postProcessAfterInstantiation(Object bean, String beanName) throws BeansException {
            return true;
        }

        @Override
        public PropertyValues postProcessPropertyValues(PropertyValues pvs, PropertyDescriptor[] pds, Object bean, String beanName) throws BeansException {
            return pvs;
        }

        @Override
        public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
            return bean;
        }

        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
            if (!onTest) return bean;

            if (beanName.equals("migrationEventDao")) {
                return new MockMigrationEventDao((MigrationEventDao) bean);
            }

            return bean;
        }

    }

}
