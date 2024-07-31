package com.ctrip.xpipe.redis.meta.server.keeper.manager;


import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerErrorCode;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.retry.RetryDelay;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
public class AddKeeperCommand extends AbstractKeeperCommand<SlaveRole>{

	private XpipeNettyClientKeyedObjectPool clientKeyedObjectPool;

	public AddKeeperCommand(KeeperContainerService keeperContainerService, KeeperTransMeta keeperTransMeta,
							XpipeNettyClientKeyedObjectPool clientKeyedObjectPool, ScheduledExecutorService scheduled,
							int timeoutMilli) {
		super(keeperContainerService, keeperTransMeta, scheduled, timeoutMilli, 1000);
		this.clientKeyedObjectPool = clientKeyedObjectPool;
	}

	public AddKeeperCommand(KeeperContainerService keeperContainerService, KeeperTransMeta keeperTransMeta,
							ScheduledExecutorService scheduled, int timeoutMilli, int checkIntervalMilli) {
		super(keeperContainerService, keeperTransMeta, scheduled, timeoutMilli, checkIntervalMilli);
	}

	@Override
	protected void preKeeperContainerOperation() {
		try {
			int usePort = keeperTransMeta.getKeeperMeta().getPort();
			KeeperInstanceMeta keeperInstanceMeta = keeperContainerService.infoPort(usePort);
			if (null != keeperInstanceMeta.getReplId() && !keeperInstanceMeta.getReplId().equals(keeperTransMeta.getReplId())) {
				getLogger().info("[preKeeperContainerOperation][{}-{}][remove unknown keeper use the same port] found:{}",
						keeperTransMeta.getClusterDbId(), keeperTransMeta.getShardDbId(), keeperInstanceMeta);
				keeperContainerService.removeKeeper(keeperInstanceMeta);
			}
		} catch (Throwable th) {
			if (getLogger().isDebugEnabled()) {
				getLogger().info("[preKeeperContainerOperation][{}-{}][fail]",
						keeperTransMeta.getClusterDbId(), keeperTransMeta.getShardDbId(), th);
			} else if (getLogger().isInfoEnabled()) {
				getLogger().info("[preKeeperContainerOperation][{}-{}][fail] {}",
						keeperTransMeta.getClusterDbId(), keeperTransMeta.getShardDbId(), th.getMessage());
			}
		}
	}

	@Override
	protected void doKeeperContainerOperation() {
		keeperContainerService.addOrStartKeeper(keeperTransMeta);
	}

	@Override
	protected boolean isSuccess(ErrorMessage<KeeperContainerErrorCode> error) {
		KeeperContainerErrorCode errorCode = error.getErrorType();
		switch(errorCode){
			case KEEPER_ALREADY_EXIST:
				return true;
			case KEEPER_ALREADY_STARTED:
				return true;
			case KEEPER_NOT_STARTED:
				return false;
			case INTERNAL_EXCEPTION:
				return false;
			case KEEPER_ALREADY_DELETED:
				return false;
			case KEEPER_ALREADY_STOPPED:
				return false;
			case KEEPER_NOT_EXIST:
				return false;
			default:
				throw new IllegalStateException("unknown state:" + errorCode);
		}
	}

	@Override
	protected Command<SlaveRole> createCheckStateCommand() {
		return new CheckStateCommand(keeperTransMeta.getKeeperMeta(), scheduled, clientKeyedObjectPool);
	}

	@Override
	protected RetryPolicy createRetryPolicy() {
		return new RetryDelay(checkIntervalMilli){
			@Override
			public boolean retry(Throwable th) {
				if(ExceptionUtils.isSocketIoException(th)){
					return false;
				}
				return true;
			}
		};
	}

	@Override
	protected void doReset() {
		
	}


	public static class CheckStateCommand extends AbstractCommand<SlaveRole> {

		private KeeperMeta keeperMeta;
		private ScheduledExecutorService scheduled;
		private XpipeNettyClientKeyedObjectPool clientKeyedObjectPool;

		public CheckStateCommand(KeeperMeta keeperMeta, ScheduledExecutorService scheduled){
			this.keeperMeta = keeperMeta;
			this.scheduled = scheduled;
		}

		public CheckStateCommand(KeeperMeta keeperMeta, ScheduledExecutorService scheduled,
								 XpipeNettyClientKeyedObjectPool clientKeyedObjectPool){
			this.keeperMeta = keeperMeta;
			this.scheduled = scheduled;
			this.clientKeyedObjectPool = clientKeyedObjectPool;
		}

			@Override
			public String getName() {
				return "[role check right command]";
			}

			@Override
			protected void doExecute() throws Exception {

				CommandFuture<Role> future;
				if(clientKeyedObjectPool != null) {
					future = new RoleCommand(clientKeyedObjectPool.getKeyPool(
							new DefaultEndPoint(keeperMeta.getIp(), keeperMeta.getPort())), scheduled).execute();
				} else {
					// for unit test purpose
					future = new RoleCommand(keeperMeta.getIp(), keeperMeta.getPort(), scheduled).execute();
				}

				future.addListener(new CommandFutureListener<Role>() {
					@Override
					public void operationComplete(CommandFuture<Role> commandFuture) throws Exception {

						if(commandFuture.isSuccess()){
							SlaveRole keeperRole = (SlaveRole)commandFuture.getNow();
							if(keeperRole.getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED){
								getLogger().info("[doExecute][success]{}", keeperRole);
								future().setSuccess(keeperRole);
							}else{
								future().setFailure(new KeeperMasterStateNotAsExpectedException(keeperMeta, keeperRole, MASTER_STATE.REDIS_REPL_CONNECTED));
							}
						}else {
							future().setFailure(commandFuture.cause());
						}
					}
				});
			}
			@Override
			protected void doReset() {

			}
		}
}
