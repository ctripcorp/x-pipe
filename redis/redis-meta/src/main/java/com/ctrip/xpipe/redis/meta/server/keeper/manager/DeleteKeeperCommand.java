package com.ctrip.xpipe.redis.meta.server.keeper.manager;


import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.netty.TcpPortCheckCommand;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerErrorCode;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
public class DeleteKeeperCommand extends AbstractKeeperCommand<Void>{
	
	public DeleteKeeperCommand(KeeperContainerService keeperContainerService, KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled,
			int timeoutMilli) {
		this(keeperContainerService, keeperTransMeta, scheduled, timeoutMilli, 1000);
	}

	public DeleteKeeperCommand(KeeperContainerService keeperContainerService, KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled,
			int timeoutMilli, int checkIntervalMilli) {
		super(keeperContainerService, keeperTransMeta, scheduled, timeoutMilli, checkIntervalMilli);
	}

	@Override
	protected void preKeeperContainerOperation() {
	}

	@Override
	protected void doKeeperContainerOperation() {
		keeperContainerService.removeKeeper(keeperTransMeta);		
	}

	@Override
	protected boolean isSuccess(ErrorMessage<KeeperContainerErrorCode> error) {
		KeeperContainerErrorCode errorCode = error.getErrorType();
		switch(errorCode){
			case KEEPER_ALREADY_EXIST:
				return false;
			case KEEPER_ALREADY_STARTED:
				return false;
			case INTERNAL_EXCEPTION:
				return false;
			case KEEPER_NOT_STARTED:
				return false;
			case KEEPER_ALREADY_DELETED:
				return true;
			case KEEPER_ALREADY_STOPPED:
				return false;
			case KEEPER_NOT_EXIST:
				return true;
			default:
				throw new IllegalStateException("unknown state:" + errorCode);
		}
	}

	@Override
	protected Command<Void> createCheckStateCommand() {
		return new DeleteCheckPortCommand(keeperTransMeta.getKeeperMeta());
	}

	@Override
	protected void doReset() {
		
	}

	public static class DeleteCheckPortCommand extends 	AbstractCommand<Void> {

		private KeeperMeta keeperMeta;

		public DeleteCheckPortCommand(KeeperMeta keeperMeta){
			this.keeperMeta = keeperMeta;
		}

		@Override
		public String getName() {
			return "[check keeper deleted]" + keeperMeta.desc();
		}

		@Override
		protected void doExecute() throws Exception {

			CommandFuture<Boolean> future = new TcpPortCheckCommand(keeperMeta.getIp(), keeperMeta.getPort()).execute();
			future.addListener(new CommandFutureListener<Boolean>() {
				@Override
				public void operationComplete(CommandFuture<Boolean> commandFuture) throws Exception {
					if(commandFuture.isSuccess()){
						future().setFailure(new DeleteKeeperStillAliveException(keeperMeta));
					}else {
						future().setSuccess();
					}
				}
			});
		}

		@Override
		protected void doReset() {

		}
	};



}
