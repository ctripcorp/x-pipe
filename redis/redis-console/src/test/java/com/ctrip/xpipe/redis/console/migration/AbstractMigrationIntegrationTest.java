package com.ctrip.xpipe.redis.console.migration;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.dao.MigrationClusterDao;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableChecker;
import com.ctrip.xpipe.redis.console.migration.command.MigrationCommandBuilder;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationEvent;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationShard;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationServiceImpl;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang.time.DateUtils;
import org.junit.Before;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.context.annotation.Bean;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.*;
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

    @Mock
    protected MigrationCommandBuilder migrationCommandBuilder;

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

        return MoreExecutors.getExitingExecutorService(
                new ThreadPoolExecutor(4,
                        maxThreads,
                        120L, TimeUnit.SECONDS,
                        new SynchronousQueue<>(),
                        XpipeThreadFactory.create(MIGRATION_EXECUTOR),
                        new ThreadPoolExecutor.CallerRunsPolicy()),

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
                event.addMigrationCluster(new DefaultMigrationCluster(migrationExecutor, scheduled, event, detail.getRedundantClusters(),
                        dcService, clusterService, mockShardService, redisService, migrationService));
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

    protected void mockSuccessCheckCommand(MigrationCommandBuilder migrationCommandBuilder) {
        when(migrationCommandBuilder.buildDcCheckCommand(anyString(), anyString(), anyString(), anyString()))
                .thenAnswer(new Answer<Command<MetaServerConsoleService.PrimaryDcCheckMessage>>() {
                    @Override
                    public Command<MetaServerConsoleService.PrimaryDcCheckMessage> answer(InvocationOnMock invocationOnMock) throws Throwable {
                        String cluster = (String) invocationOnMock.getArguments()[0];
                        String shard = (String) invocationOnMock.getArguments()[1];
                        String dc = (String) invocationOnMock.getArguments()[2];
                        String newPrimaryDc = (String) invocationOnMock.getArguments()[3];
                        return new AbstractCommand<MetaServerConsoleService.PrimaryDcCheckMessage>() {

                            @Override
                            public String getName() {
                                return String.format("Mocked-CheckSuccess-%s-%s-%s-%s", cluster, shard, dc, newPrimaryDc);
                            }

                            @Override
                            protected void doExecute() throws Exception {
                                Thread.sleep(randomInt(5, 10));
//                                logger.info("[doExecute][set result]{}, {}, {}", cluster, shard, future());
                                future().setSuccess(
                                        new MetaServerConsoleService.PrimaryDcCheckMessage(MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT.SUCCESS, "Check success"));
                            }

                            @Override
                            protected void doReset() {

                            }
                        };
                    }
                });
    }

    protected void mockSuccessPrevPrimaryDcCommand(MigrationCommandBuilder migrationCommandBuilder) {
        when(migrationCommandBuilder.buildPrevPrimaryDcCommand(anyString(), anyString(), anyString()))
                .thenAnswer(new Answer<Command<MetaServerConsoleService.PreviousPrimaryDcMessage>>() {
                    @Override
                    public Command<MetaServerConsoleService.PreviousPrimaryDcMessage> answer(InvocationOnMock invocationOnMock) throws Throwable {
                        String cluster = (String) invocationOnMock.getArguments()[0];
                        String shard = (String) invocationOnMock.getArguments()[1];
                        String newPrimaryDc = (String) invocationOnMock.getArguments()[2];

                        return new AbstractCommand<MetaServerConsoleService.PreviousPrimaryDcMessage>() {

                            @Override
                            public String getName() {
                                return String.format("Mocked-CheckSuccess-%s-%s-%s", cluster, shard, newPrimaryDc);
                            }

                            @Override
                            protected void doExecute() throws Exception {
                                Thread.sleep(randomInt(65, 100));
//                                logger.info("[doExecute][set result]{}, {}, {}", cluster, shard, future());
                                future().setSuccess(
                                        new MetaServerConsoleService.PreviousPrimaryDcMessage(new HostPort("127.0.0.1", 6379), null, "Prev success"));
                            }

                            @Override
                            protected void doReset() {

                            }
                        };
                    }
                });
    }

    protected void mockSuccessNewPrimaryDcCommand(MigrationCommandBuilder migrationCommandBuilder) {
        when(migrationCommandBuilder.buildNewPrimaryDcCommand(anyString(), anyString(), anyString(), anyObject()))
                .thenAnswer(new Answer<Command<MetaServerConsoleService.PrimaryDcChangeMessage>>() {
                    @Override
                    public Command<MetaServerConsoleService.PrimaryDcChangeMessage> answer(InvocationOnMock invocationOnMock) throws Throwable {
                        String cluster = (String) invocationOnMock.getArguments()[0];
                        String shard = (String) invocationOnMock.getArguments()[1];
                        String newPrimaryDc = (String) invocationOnMock.getArguments()[2];
                        return new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {
                            @Override
                            public String getName() {
                                return String.format("Mocked-CheckSuccess-%s-%s-%s", cluster, shard, newPrimaryDc);
                            }

                            @Override
                            protected void doExecute() throws Exception {
                                Thread.sleep(randomInt(32, 100));
//                                logger.info("[doExecute][set result]{}, {}, {}", cluster, shard, future());
                                future().setSuccess(
                                        new MetaServerConsoleService.PrimaryDcChangeMessage("New success", "127.0.0.1", randomPort()));
                            }

                            @Override
                            protected void doReset() {

                            }
                        };
                    }
                });

    }

    protected void mockSuccessOtherDcCommand(MigrationCommandBuilder migrationCommandBuilder) {
        when(migrationCommandBuilder.buildOtherDcCommand(anyString(), anyString(), anyString(), anyString()))
                .thenAnswer(new Answer<Command<MetaServerConsoleService.PrimaryDcChangeMessage>>() {
                    @Override
                    public Command<MetaServerConsoleService.PrimaryDcChangeMessage> answer(InvocationOnMock invocationOnMock) throws Throwable {
                        String cluster = (String) invocationOnMock.getArguments()[0];
                        String shard = (String) invocationOnMock.getArguments()[1];
                        String newPrimaryDc = (String) invocationOnMock.getArguments()[2];
                        return new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {
                            @Override
                            public String getName() {
                                return String.format("Mocked-CheckSuccess-%s-%s-%s", cluster, shard, newPrimaryDc);
                            }

                            @Override
                            protected void doExecute() throws Exception {
                                Thread.sleep(randomInt(34, 100));
//                                logger.info("[doExecute][set result]{}, {}, {}", cluster, shard, future());
                                future().setSuccess(
                                        new MetaServerConsoleService.PrimaryDcChangeMessage(MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT.SUCCESS, "Other success"));
                            }

                            @Override
                            protected void doReset() {

                            }
                        };
                    }
                });

    }
}
