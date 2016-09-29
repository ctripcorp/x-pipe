package com.ctrip.xpipe.redis.meta.server.keeper.manager;


import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerErrorCode;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.KeeperRole;
import com.ctrip.xpipe.retry.RetryDelay;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
public class AddKeeperCommand extends AbstractKeeperCommand<KeeperRole>{

	public AddKeeperCommand(KeeperContainerService keeperContainerService, KeeperTransMeta keeperTransMeta,
			int timeoutMilli) {
		super(keeperContainerService, keeperTransMeta, timeoutMilli, 1000);
	}

	public AddKeeperCommand(KeeperContainerService keeperContainerService, KeeperTransMeta keeperTransMeta,
			int timeoutMilli, int checkIntervalMilli) {
		super(keeperContainerService, keeperTransMeta, timeoutMilli, checkIntervalMilli);
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
	protected Command<KeeperRole> createCheckStateCommand() {
		return new AbstractCommand<KeeperRole>() {

			@Override
			public String getName() {
				return "[role check right command]";
			}

			@Override
			protected void doExecute() throws Exception {
				
				KeeperRole keeperRole = (KeeperRole) new RoleCommand(keeperTransMeta.getKeeperMeta().getIp(), keeperTransMeta.getKeeperMeta().getPort()).execute().get();
				if(keeperRole.getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED){
					logger.info("[doExecute][success]{}", keeperRole);
					future().setSuccess(keeperRole);
				}else{
					future().setFailure(new KeeperMasterStateNotAsExpectedException(keeperTransMeta.getKeeperMeta(), keeperRole, MASTER_STATE.REDIS_REPL_CONNECTED));
				}
			}
			@Override
			protected void doReset() {
				
			}
		};
	}
	
	@Override
	protected RetryPolicy createRetryPolicy() {
		return new RetryDelay(checkIntervalMilli){
			@Override
			public boolean retry(Throwable th) {
				if(ExceptionUtils.isIoException(th)){
					return false;
				}
				return true;
			}
		};
	}

	@Override
	protected void doReset() {
		
	}
}
