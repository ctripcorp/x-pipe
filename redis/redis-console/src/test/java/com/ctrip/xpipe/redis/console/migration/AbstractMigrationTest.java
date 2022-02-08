package com.ctrip.xpipe.redis.console.migration;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.migration.command.MigrationCommandBuilder;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcCheckMessage;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.function.Supplier;

import static org.mockito.Mockito.*;

/**
 * @author shyin
 *
 *         Dec 20, 2016
 */
public class AbstractMigrationTest extends AbstractConsoleIntegrationTest {

	@Autowired
	protected DcService dcService;
	@Autowired
	protected ClusterService clusterService;
	@Autowired
	protected ShardService shardService;
	@Autowired
	protected RedisService redisService;
	@Autowired
	protected MigrationService migrationService;

	protected void mockSuccessCheckCommand(MigrationCommandBuilder migrationCommandBuilder, String cluster,
			String shard, String dc, String newPrimaryDc) {
		when(migrationCommandBuilder.buildDcCheckCommand(cluster, shard, dc, newPrimaryDc))
				.thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcCheckMessage>() {

					@Override
					public String getName() {
						return String.format("Mocked-CheckSuccess-%s-%s-%s-%s", cluster, shard, dc, newPrimaryDc);
					}

					@Override
					protected void doExecute() throws Exception {
						logger.info("[doExecute][set result]{}, {}, {}", cluster, shard, future());
						future().setSuccess(
								new PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT.SUCCESS, "Check success"));
					}

					@Override
					protected void doReset() {
					}
				});
	}

	protected void mockFailCheckCommand(MigrationCommandBuilder migrationCommandBuilder, String cluster, String shard,
			String dc, String newPrimaryDc) {
		when(migrationCommandBuilder.buildDcCheckCommand(cluster, shard, dc, newPrimaryDc))
				.thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcCheckMessage>() {

					@Override
					public String getName() {
						return String.format("Mocked-CheckSuccess-%s-%s-%s-%s", cluster, shard, dc, newPrimaryDc);
					}

					@Override
					protected void doExecute() throws Exception {
						future().setSuccess(
								new PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT.FAIL, "Check fail with some reason"));
					}

					@Override
					protected void doReset() {
					}
				});
	}

	protected void mockFailCheckCommand(MigrationCommandBuilder migrationCommandBuilder, String cluster, String shard,
			String dc, String newPrimaryDc, Throwable ex) {
		when(migrationCommandBuilder.buildDcCheckCommand(cluster, shard, dc, newPrimaryDc))
				.thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcCheckMessage>() {

					@Override
					public String getName() {
						return String.format("Mocked-CheckFail-%s-%s-%s-%s", cluster, shard, dc, newPrimaryDc);
					}

					@Override
					protected void doExecute() throws Exception {
						future().setFailure(ex);
					}

					@Override
					protected void doReset() {
					}
				});
	}

	protected void mockSuccessPrevPrimaryDcCommand(MigrationCommandBuilder migrationCommandBuilder, String cluster,
			String shard, String prevPrimaryDc) {
		when(migrationCommandBuilder.buildPrevPrimaryDcCommand(cluster, shard, prevPrimaryDc))
				.thenReturn(new AbstractCommand<MetaServerConsoleService.PreviousPrimaryDcMessage>() {
					@Override
					public String getName() {
						return String.format("Mocked-PrevSuccess-%s-%s-%s", cluster, shard, prevPrimaryDc);
					}

					@Override
					protected void doExecute() throws Exception {
						future().setSuccess(
								new MetaServerConsoleService.PreviousPrimaryDcMessage(new HostPort("127.0.0.1", 6379), null, "Prev success"));
					}

					@Override
					protected void doReset() {
					}
				});
	}

	protected void mockFailPrevPrimaryDcCommand(MigrationCommandBuilder migrationCommandBuilder, String cluster,
			String shard, String prevPrimaryDc, Throwable ex) {
		when(migrationCommandBuilder.buildPrevPrimaryDcCommand(cluster, shard, prevPrimaryDc))
				.thenReturn(new AbstractCommand<MetaServerConsoleService.PreviousPrimaryDcMessage>() {
					@Override
					public String getName() {
						return String.format("Mocked-PrevFail-%s-%s-%s", cluster, shard, prevPrimaryDc);
					}

					@Override
					protected void doExecute() throws Exception {
						future().setFailure(ex);
					}

					@Override
					protected void doReset() {
					}
				});
	}

	protected void mockSuccessNewPrimaryDcCommand(MigrationCommandBuilder migrationCommandBuilder, String cluster,
			String shard, String newPrimaryDc) {

		when(migrationCommandBuilder.buildNewPrimaryDcCommand(eq(cluster), eq(shard), eq(newPrimaryDc), any(Supplier.class)))
				.thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {

					@Override
					public String getName() {
						return String.format("Mocked-NewSuccess-%s-%s-%s", cluster, shard, newPrimaryDc);
					}

					@Override
					protected void doExecute() throws Exception {
						future().setSuccess(
								new PrimaryDcChangeMessage("New success", "127.0.0.1", randomPort()));
					}

					@Override
					protected void doReset() {
					}
				});
	}
	
	protected void mockFailNewPrimaryDcCommand(MigrationCommandBuilder migrationCommandBuilder, String cluster,
			String shard, String newPrimaryDc) {
		when(migrationCommandBuilder.buildNewPrimaryDcCommand(eq(cluster), eq(shard), eq(newPrimaryDc), any(Supplier.class)))
				.thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {

					@Override
					public String getName() {
						return String.format("Mocked-NewSuccess-%s-%s-%s", cluster, shard, newPrimaryDc);
					}

					@Override
					protected void doExecute() throws Exception {
						future().setSuccess(
								new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.FAIL, "New fail with some reason"));
					}

					@Override
					protected void doReset() {
					}
				});
	}

	protected void mockFailNewPrimaryDcCommand(MigrationCommandBuilder migrationCommandBuilder, String cluster,
			String shard, String newPrimaryDc, Throwable ex) {
		when(migrationCommandBuilder.buildNewPrimaryDcCommand(eq(cluster), eq(shard), eq(newPrimaryDc), any(Supplier.class)))
				.thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {

					@Override
					public String getName() {
						return String.format("Mocked-NewFail-%s-%s-%s", cluster, shard, newPrimaryDc);
					}

					@Override
					protected void doExecute() throws Exception {
						future().setFailure(ex);
					}

					@Override
					protected void doReset() {
					}
				});
	}

	protected void mockFailThenSuccessNewPrimaryDcCommand(MigrationCommandBuilder migrationCommandBuilder, String cluster,
														  String shard, String newPrimaryDc, Throwable ex) {
		when(migrationCommandBuilder.buildNewPrimaryDcCommand(eq(cluster), eq(shard), eq(newPrimaryDc), any(Supplier.class)))
				.thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {

					@Override
					public String getName() {
						return String.format("Mocked-NewFail-%s-%s-%s", cluster, shard, newPrimaryDc);
					}

					@Override
					protected void doExecute() throws Exception {
						future().setFailure(ex);
					}

					@Override
					protected void doReset() {
					}
				})
				.thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {

					@Override
					public String getName() {
						return String.format("Mocked-NewSuccess-%s-%s-%s", cluster, shard, newPrimaryDc);
					}

					@Override
					protected void doExecute() throws Exception {
						future().setSuccess(
								new PrimaryDcChangeMessage("New success", "127.0.0.1", randomPort()));
					}

					@Override
					protected void doReset() {
					}
				});

	}


	protected void mockSuccessOtherDcCommand(MigrationCommandBuilder migrationCommandBuilder, String cluster,
			String shard, String newPrimaryDc, String otherDc) {
		when(migrationCommandBuilder.buildOtherDcCommand(cluster, shard, newPrimaryDc, otherDc))
				.thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {

					@Override
					public String getName() {
						return String.format("Mocked-OtherSuccess-%s-%s-%s-%s", cluster, shard, newPrimaryDc, otherDc);
					}

					@Override
					protected void doExecute() throws Exception {
						future().setSuccess(
								new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.SUCCESS, "Other success"));
					}

					@Override
					protected void doReset() {
					}
				});
	}
	
	protected void mockFailOtherDcCommand(MigrationCommandBuilder migrationCommandBuilder, String cluster,
			String shard, String newPrimaryDc, String otherDc) {
		when(migrationCommandBuilder.buildOtherDcCommand(cluster, shard, newPrimaryDc, otherDc))
				.thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {

					@Override
					public String getName() {
						return String.format("Mocked-OtherSuccess-%s-%s-%s-%s", cluster, shard, newPrimaryDc, otherDc);
					}

					@Override
					protected void doExecute() throws Exception {
						future().setSuccess(
								new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.FAIL, "Other fail with some reason"));
					}

					@Override
					protected void doReset() {
					}
				});
	}

	protected void mockFailOtherDcCommand(MigrationCommandBuilder migrationCommandBuilder, String cluster, String shard,
			String newPrimaryDc, String otherDc, Throwable ex) {
		when(migrationCommandBuilder.buildOtherDcCommand(cluster, shard, newPrimaryDc, otherDc))
				.thenReturn(new AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>() {

					@Override
					public String getName() {
						return String.format("Mocked-OtherFail-%s-%s-%s-%s", cluster, shard, newPrimaryDc, otherDc);
					}

					@Override
					protected void doExecute() throws Exception {
						future().setFailure(ex);
					}

					@Override
					protected void doReset() {
					}
				});
	}
	
	protected void mockSuccessRollBackCommand(MigrationCommandBuilder migrationCommandBuilder, String cluster, String shard,
			String prevPrimaryDc) {
		when(migrationCommandBuilder.buildRollBackCommand(cluster, shard, prevPrimaryDc))
			.thenReturn(new AbstractCommand<MetaServerConsoleService.PreviousPrimaryDcMessage>(){

				@Override
				public String getName() {
					return String.format("Mocked-RollBackSuccess-%s-%s-%s", cluster, shard, prevPrimaryDc);
				}

				@Override
				protected void doExecute() throws Exception {
					future().setSuccess();
				}

				@Override
				protected void doReset() {
				}
				
			});
	}
	
	protected void mockFailRollBackCommand(MigrationCommandBuilder migrationCommandBuilder, String cluster, String shard,
			String prevPrimaryDc) {
		when(migrationCommandBuilder.buildRollBackCommand(cluster, shard, prevPrimaryDc))
			.thenReturn(new AbstractCommand<MetaServerConsoleService.PreviousPrimaryDcMessage>(){

				@Override
				public String getName() {
					return String.format("Mocked-RollBackSuccess-%s-%s-%s", cluster, shard, prevPrimaryDc);
				}

				@Override
				protected void doExecute() throws Exception {
					future().setFailure(new Throwable("mocked throwable"));
				}

				@Override
				protected void doReset() {
				}
				
			});
	}
}
