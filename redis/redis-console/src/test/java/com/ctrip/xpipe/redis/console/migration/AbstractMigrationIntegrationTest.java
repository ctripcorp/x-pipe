package com.ctrip.xpipe.redis.console.migration;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.dao.MigrationClusterDao;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableChecker;
import com.ctrip.xpipe.redis.console.migration.command.MigrationCommandBuilder;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationEvent;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationShard;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationServiceImpl;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static org.mockito.Mockito.*;

public class AbstractMigrationIntegrationTest extends AbstractTest {
    @Mock
    protected MigrationEventDao migrationEventDao;

    @Mock
    protected MigrationEventManager migrationEventManager;

    @Mock
    protected ClusterService clusterService;

    @Mock
    protected DcClusterService dcClusterService;

    @Mock
    protected AlertManager alertManager;

    @Mock
    protected DcService dcService;

    @Mock
    protected AzGroupClusterRepository azGroupClusterRepository;

    @Mock
    protected AzGroupCache azGroupCache;

    @Mock
    protected MigrationClusterDao migrationClusterDao;

    @Mock
    protected MigrationSystemAvailableChecker checker;

    @Mock
    protected ConfigService configService;

    @Mock
    protected MigrationShardTblDao migrationShardTblDao;

    @Mock
    protected ShardService shardService;

    @Mock
    protected RedisService redisService;

    protected MigrationCommandBuilder migrationCommandBuilder = new MockMigartionCommandBuilder();;

    @Mock
    protected OuterClientService outerClientService;

    @InjectMocks
    protected MigrationServiceImpl migrationService = new MigrationServiceImpl();

    protected String fromIdc = "SHAJQ", toIdc = "SHAOY";

    protected long fromIdcId = 1, toIdcId = 2;

    protected ExecutorService migrationExecutor = getMigrationlExecutor();

    public static final String MIGRATION_EXECUTOR = "MIGRATION_EXECUTOR";

    public static final int maxThreads = 512;

    public ExecutorService getMigrationlExecutor() {
        //63337
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(maxThreads,
                maxThreads,
                120L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(128),
                XpipeThreadFactory.create(MIGRATION_EXECUTOR),
                new ThreadPoolExecutor.CallerRunsPolicy());
        //109499
//        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(4,
//                maxThreads,
//                120L, TimeUnit.SECONDS,
//                new SynchronousQueue<>(),
//                XpipeThreadFactory.create(MIGRATION_EXECUTOR),
//                new ThreadPoolExecutor.CallerRunsPolicy());
        threadPool.allowCoreThreadTimeOut(true);
        return MoreExecutors.getExitingExecutorService(
                threadPool,
                AbstractSpringConfigContext.THREAD_POOL_TIME_OUT, TimeUnit.SECONDS);
    }

    @Before
    public void beforeAbstractMigrationIntegrationTest() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(configService.ignoreMigrationSystemAvailability()).thenReturn(false);
        when(checker.getResult()).thenReturn(new MigrationSystemAvailableChecker.MigrationSystemAvailability(true, ""));
        when(clusterService.find(anyLong())).thenAnswer(new Answer<ClusterTbl>() {
            @Override
            public ClusterTbl answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (isDelay()) {
                    Thread.sleep(randomInt(5, 25));
                }
                long clusterId = (Long) invocationOnMock.getArguments()[0];
                return new ClusterTbl().setId(clusterId).setActivedcId(fromIdcId).setClusterName("cluster-" + clusterId);
            }
        });
        when(dcService.findClusterRelatedDc(anyString())).thenAnswer(new Answer<List<DcTbl>>() {
            @Override
            public List<DcTbl> answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (isDelay()) {
                    Thread.sleep(randomInt(5, 29));
                }
                return Lists.newArrayList(new DcTbl().setZoneId(1L).setId(fromIdcId).setDcName(fromIdc),
                        new DcTbl().setZoneId(1L).setId(toIdcId).setDcName(toIdc));
            }
        });
        when(outerClientService.getClusterInfo(anyString())).thenAnswer(new Answer<OuterClientService.ClusterInfo>() {
            @Override
            public OuterClientService.ClusterInfo answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (isDelay()) {
                    Thread.sleep(3, 100);
                }
                OuterClientService.ClusterInfo clusterInfo = new OuterClientService.ClusterInfo();
                clusterInfo.setGroups(Lists.newArrayList(new OuterClientService.GroupInfo()));
                return clusterInfo;
            }
        });
        when(outerClientService.doMigrationPublish(anyString(), anyString(), any())).thenAnswer(new Answer<OuterClientService.MigrationPublishResult>() {
            @Override
            public OuterClientService.MigrationPublishResult answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (isDelay()) {
                    Thread.sleep(randomInt(103, 300));
                }
                return new OuterClientService.MigrationPublishResult();
            }
        });
        migrationService = spy(migrationService);
    }

    private boolean delay = true;

    protected void setDelay(boolean delay) {
        this.delay = delay;
    }
    protected boolean isDelay() {
        return delay;
    }

    protected MigrationEvent loadMigrationEvent(List<MigrationEventTbl> details) {
        MigrationEvent event = new DefaultMigrationEvent(details.get(0));
        List<ShardTbl> shards = Lists.newArrayListWithCapacity(10);
        for(MigrationEventTbl detail : details) {
            MigrationShardTbl shard = detail.getRedundantShards();
            shards.add(new ShardTbl().setId(shard.getId()).setClusterName("cluster-" + detail.getRedundantClusters().getId())
                    .setShardName("shard-" + shard.getId()).setSetinelMonitorName("monitor")
                    .setClusterId(detail.getRedundantClusters().getId()));
        }
        ShardService mockShardService = mock(ShardService.class);

        when(mockShardService.findAllByClusterName(anyString())).thenAnswer(new Answer<List<ShardTbl>>() {
            @Override
            public List<ShardTbl> answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(randomInt(3, 20));
                return shards;
            }
        });
        for(MigrationEventTbl detail : details) {
            MigrationClusterTbl cluster = detail.getRedundantClusters();
            MigrationShardTbl shard = detail.getRedundantShards();

            if(null == event.getMigrationCluster(cluster.getClusterId())) {
                event.addMigrationCluster(new DefaultMigrationCluster(migrationExecutor, scheduled, event,
                    detail.getRedundantClusters(), azGroupClusterRepository, azGroupCache, dcService, clusterService,
                    mockShardService, redisService, migrationService));
            }
            MigrationCluster migrationCluster = event.getMigrationCluster(cluster.getClusterId());
            ((DefaultMigrationCluster) migrationCluster).setOuterClientService(outerClientService);
            ShardTbl shardTbl = migrationCluster.getClusterShards().get(shard.getId());
            migrationCluster.addNewMigrationShard(new DefaultMigrationShard(migrationCluster, shard,
                    shardTbl,
                    migrationCluster.getClusterDcs(),
                    migrationService, migrationCommandBuilder));
        }

        return event;
    }

    protected List<MigrationEventTbl> migrationEventDetails(long eventId) {
        List<MigrationEventTbl> details = Lists.newLinkedList();
        long clusterId = eventId;
        for (int i = 0; i < randomInt(4, 10); i++) {
            long shardId = (long) randomInt(0, Integer.MAX_VALUE - 2);
            MigrationShardTbl shardTbl = new MigrationShardTbl().setId(shardId).setShardId(shardId).setMigrationClusterId(clusterId).setDeleted(0);
            details.add(createMigrationEventTbl(eventId, clusterId).setRedundantShards(shardTbl));
        }
        return details;
    }

    protected MigrationEventTbl createMigrationEventTbl(long eventId, long clusterId) {
        MigrationEventTbl migrationEventTbl = new MigrationEventTbl();
        MigrationClusterTbl migrationClusterTbl = new MigrationClusterTbl().setId(clusterId).setClusterId(clusterId)
                .setSourceDcId(fromIdcId).setDestinationDcId(toIdcId).setMigrationEventId(eventId)
                .setStartTime(new Date()).setStatus("Checking");
        migrationEventTbl.setCount(1).setDataChangeLastTime(new Date()).setDeleted(false)
                .setEventTag(DateTimeUtils.currentTimeAsString()).setId(eventId)
                .setRedundantClusters(migrationClusterTbl)
                .setStartTime(new Date());
        return migrationEventTbl;
    }


    public static class MockMigartionCommandBuilder extends AbstractService implements MigrationCommandBuilder {

        private Random random = new Random();

        protected int randomInt(int start, int end) {
            return start + random.nextInt(end - start + 1);
        }

        @Override
        public Command<MetaServerConsoleService.PrimaryDcCheckMessage> buildDcCheckCommand(String cluster, String shard, String dc, String newPrimaryDc) {
            return new AbstractCommand<MetaServerConsoleService.PrimaryDcCheckMessage>() {

                @Override
                public String getName() {
                    return String.format("Mocked-CheckSuccess-%s-%s-%s-%s", cluster, shard, dc, newPrimaryDc);
                }

                @Override
                protected void doExecute() throws Exception {
                    Thread.sleep(randomInt(5, 10));
                    future().setSuccess(
                            new MetaServerConsoleService.PrimaryDcCheckMessage(MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT.SUCCESS, "Check success"));
                }

                @Override
                protected void doReset() {

                }
            };
        }

        @Override
        public Command<MetaServerConsoleService.PreviousPrimaryDcMessage> buildPrevPrimaryDcCommand(String cluster, String shard, String prevPrimaryDc) {
            return new AbstractCommand<MetaServerConsoleService.PreviousPrimaryDcMessage>() {

                @Override
                public String getName() {
                    return String.format("Mocked-CheckSuccess-%s-%s-%s", cluster, shard, prevPrimaryDc);
                }

                @Override
                protected void doExecute() throws Exception {
//                    Thread.sleep(3200);
                    try {
                        restTemplate.getForEntity("http://10.0.0.1:8080", Object.class);
                    } catch (Exception ignore) {

                    }
                    future().setSuccess(
                            new MetaServerConsoleService.PreviousPrimaryDcMessage(new HostPort("127.0.0.1", 6379), null, "Prev success"));
                }

                @Override
                protected void doReset() {

                }
            };
        }

        @Override
        public Command<MetaServerConsoleService.PrimaryDcChangeMessage> buildNewPrimaryDcCommand(String cluster, String shard, String newPrimaryDc, MetaServerConsoleService.PreviousPrimaryDcMessage previousPrimaryDcMessage) {
            return new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {
                @Override
                public String getName() {
                    return String.format("Mocked-CheckSuccess-%s-%s-%s", cluster, shard, newPrimaryDc);
                }

                @Override
                protected void doExecute() throws Exception {
                    Thread.sleep(randomInt(32, 100));
                    future().setSuccess(
                            new MetaServerConsoleService.PrimaryDcChangeMessage("New success", "127.0.0.1", randomPort()));
                }

                @Override
                protected void doReset() {

                }
            };
        }

        @Override
        public Command<MetaServerConsoleService.PrimaryDcChangeMessage> buildOtherDcCommand(String cluster, String shard, String primaryDc, String executeDc) {
            return new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {
                @Override
                public String getName() {
                    return String.format("Mocked-CheckSuccess-%s-%s-%s", cluster, shard, primaryDc);
                }

                @Override
                protected void doExecute() throws Exception {
//                    Thread.sleep(3200);
                    try {
                        restTemplate.getForEntity("http://10.0.0.1:8080", Object.class);
                    } catch (Exception ignore) {

                    }
                    future().setSuccess(
                            new MetaServerConsoleService.PrimaryDcChangeMessage(MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT.SUCCESS, "Other success"));
                }

                @Override
                protected void doReset() {

                }
            };
        }

        @Override
        public Command<MetaServerConsoleService.PreviousPrimaryDcMessage> buildRollBackCommand(String cluster, String shard, String prevPrimaryDc) {
            return null;
        }

    }
}
