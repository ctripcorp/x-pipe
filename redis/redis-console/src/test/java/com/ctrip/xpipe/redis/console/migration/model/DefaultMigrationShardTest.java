package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.migration.command.MigrationCommandBuilder;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationShard;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

/**
 * Created by Shyin on 10/12/2016.
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultMigrationShardTest extends AbstractConsoleTest {

    private MigrationShard migrationShard;

    @Mock
    private MigrationCluster mockedMigrationCluster;
    private MigrationShardTbl mockedMigrationShard;
    private ShardTbl mockedCurrentShard;
    private Map<Long, DcTbl> mockedDcs = new HashMap<>(3);

    @Mock
    private MigrationService mockedMigrationService;
    @Mock
    private RedisService mockedRedisService;
    @Mock
    private MigrationCommandBuilder mockedCommandBuilder;

    @Before
    public void setUp() {
        prepareMockData();
    }

    @Test
    public void testCheckSuccess() {
        when(mockedCommandBuilder.buildDcCheckCommand("test-cluster", "test-shard", "dc-b", "dc-b"))
                .thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcCheckMessage>() {
                    @Override
                    protected void doExecute() throws Exception {
                        future().setSuccess(new MetaServerConsoleService.PrimaryDcCheckMessage(
                                MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT.SUCCESS, "Test-success"));
                    }

                    @Override
                    protected void doReset() {
                    }

                    @Override
                    public String getName() {
                        return "testCheckSuccess";
                    }
                });

        Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
        migrationShard.doCheck();
        verify(mockedCommandBuilder, times(1)).buildDcCheckCommand("test-cluster", "test-shard", "dc-b", "dc-b");
        verify(mockedMigrationService, times(1)).updateMigrationShardLogById(anyLong(), anyString());
        Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
    }

    @Test
    public void testCheckFail(){
        when(mockedCommandBuilder.buildDcCheckCommand("test-cluster", "test-shard", "dc-b", "dc-b"))
                .thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcCheckMessage>() {
                    @Override
                    protected void doExecute() throws Exception {
                        future().setSuccess(new MetaServerConsoleService.PrimaryDcCheckMessage(
                                MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT.FAIL, "Test-fail"));
                    }

                    @Override
                    protected void doReset() {
                    }

                    @Override
                    public String getName() {
                        return "testCheckFail";
                    }
                });

        Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
        migrationShard.doCheck();
        verify(mockedCommandBuilder, times(1)).buildDcCheckCommand("test-cluster", "test-shard", "dc-b", "dc-b");
        verify(mockedMigrationService, times(1)).updateMigrationShardLogById(anyLong(), anyString());
        Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.CHECK));
    }

    @Test
    public void testMigrateSuccess() {
        when(mockedCommandBuilder.buildPrevPrimaryDcCommand("test-cluster", "test-shard", "dc-a"))
                .thenReturn(new AbstractCommand<MetaServerConsoleService.PreviousPrimaryDcMessage>() {
                    @Override
                    protected void doExecute() throws Exception {
                        future().setSuccess(new MetaServerConsoleService.PreviousPrimaryDcMessage(
                                new HostPort("127.0.0.1", 0), null, "Test-Success"));
                    }

                    @Override
                    protected void doReset() {
                    }

                    @Override
                    public String getName() {
                        return "testPrevPrimaryDcSuccess";
                    }
                });
        when(mockedCommandBuilder.buildNewPrimaryDcCommand(eq("test-cluster"), eq("test-shard"), eq("dc-b"), anyObject()))
                .thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {
                    @Override
                    protected void doExecute() throws Exception {
                        future().setSuccess(new MetaServerConsoleService.PrimaryDcChangeMessage(
                                MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT.SUCCESS, "Test-success"));
                    }

                    @Override
                    protected void doReset() {
                    }

                    @Override
                    public String getName() {
                        return "testNewPrimaryDcSuccess";
                    }
                });
        when(mockedCommandBuilder.buildOtherDcCommand("test-cluster", "test-shard", "dc-b", "dc-a"))
        .thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {
            @Override
            protected void doExecute() throws Exception {
                future().setSuccess(new MetaServerConsoleService.PrimaryDcChangeMessage(
                        MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT.SUCCESS, "Test-success"));
            }

            @Override
            protected void doReset() {
            }

            @Override
            public String getName() {
                return "testNewPrimaryDcSuccess";
            }
        });

        Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
        Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
        Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
        Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
        Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));

        migrationShard.doMigrate();
//        verify(mockedMigrationService, times(5)).updateMigrationShardLogById(anyLong(), anyString());
        Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
        Assert.assertEquals(ShardMigrationResultStatus.SUCCESS, migrationShard.getShardMigrationResult().getStatus());
    }

    @Test
    public void testMigrationFail() {
        when(mockedCommandBuilder.buildPrevPrimaryDcCommand("test-cluster", "test-shard", "dc-a"))
                .thenReturn(new AbstractCommand<MetaServerConsoleService.PreviousPrimaryDcMessage>() {
                    @Override
                    protected void doExecute() throws Exception {
                        MetaServerConsoleService.PreviousPrimaryDcMessage message = new MetaServerConsoleService.PreviousPrimaryDcMessage();
                        future().setSuccess(message);
                    }

                    @Override
                    protected void doReset() {
                    }

                    @Override
                    public String getName() {
                        return "testPrevPrimaryDcSuccess";
                    }
                });
        when(mockedCommandBuilder.buildNewPrimaryDcCommand(eq("test-cluster"), eq("test-shard"), eq("dc-b"), anyObject()))
                .thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {
                    @Override
                    protected void doExecute() throws Exception {
                        future().setSuccess(new MetaServerConsoleService.PrimaryDcChangeMessage(
                                MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT.FAIL, "Test-fail"));
                    }

                    @Override
                    protected void doReset() {
                    }

                    @Override
                    public String getName() {
                        return "testNewPrimaryDcFail";
                    }
                });
        when(mockedCommandBuilder.buildOtherDcCommand("test-cluster", "test-shard", "dc-b", "dc-a"))
        	.thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {
            @Override
            protected void doExecute() throws Exception {
                future().setSuccess(new MetaServerConsoleService.PrimaryDcChangeMessage(
                        MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT.SUCCESS, "Test-success"));
            }

            @Override
            protected void doReset() {
            }

            @Override
            public String getName() {
                return "testNewPrimaryDcSuccess";
            }
        });

        Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
        Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
        Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
        Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
        Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_OTHER_DC));

        migrationShard.doMigrate();
//        verify(mockedMigrationService, times(3)).updateMigrationShardLogById(anyLong(), anyString());
        Assert.assertTrue(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_PREVIOUS_PRIMARY_DC));
        Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
        Assert.assertFalse(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE));
        Assert.assertEquals(ShardMigrationResultStatus.FAIL, migrationShard.getShardMigrationResult().getStatus());
    }


    private void prepareMockData() {

        String clusterName = "test-cluster";
        when(mockedMigrationCluster.getCurrentCluster()).thenReturn((new ClusterTbl()).setId(1)
                .setClusterName(clusterName).setActivedcId(1L));
        when(mockedMigrationCluster.clusterName()).thenReturn(clusterName);
        when(mockedMigrationCluster.getMigrationCluster()).thenReturn((new MigrationClusterTbl()).setClusterId(1)
                .setDestinationDcId(2L));
        when(mockedMigrationCluster.getRedisService()).thenReturn(mockedRedisService);
        final AtomicInteger cnt = new AtomicInteger(0);
        doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				Object[] args = invocation.getArguments();
				System.out.println(args[0]);
				if(cnt.incrementAndGet() == 2) {
					if(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)) {
						migrationShard.doMigrateOtherDc();
					}
				};
				return null;
			}
		}).when(mockedMigrationCluster).update(anyObject(), anyObject());

        mockedMigrationShard = (new MigrationShardTbl()).setId(1).setKeyId(1).setShardId(1).setMigrationClusterId(1);
        mockedCurrentShard = (new ShardTbl()).setId(1).setKeyId(1).setShardName("test-shard").setClusterId(1)
                .setSetinelMonitorName("test-shard-monitor");

        mockedDcs.put(1L, (new DcTbl()).setId(1).setKeyId(1).setDcName("dc-a"));
        mockedDcs.put(2L, (new DcTbl()).setId(2).setKeyId(2).setDcName("dc-b"));

        migrationShard = new DefaultMigrationShard(mockedMigrationCluster, mockedMigrationShard, mockedCurrentShard,
                mockedDcs, mockedMigrationService, mockedCommandBuilder);

    }


}
