package com.ctrip.xpipe.redis.meta.server.keeper.manager;


import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerErrorCode;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.KeeperRole;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
public class AddKeeperCommand extends AbstractKeeperCommand<KeeperRole>{

	public AddKeeperCommand(KeeperContainerService keeperContainerService, KeeperInstanceMeta keeperInstanceMeta,
			int timeoutMilli) {
		super(keeperContainerService, keeperInstanceMeta, timeoutMilli, 1000);
	}

	public AddKeeperCommand(KeeperContainerService keeperContainerService, KeeperInstanceMeta keeperInstanceMeta,
			int timeoutMilli, int checkIntervalMilli) {
		super(keeperContainerService, keeperInstanceMeta, timeoutMilli, checkIntervalMilli);
	}


	@Override
	public String getName() {
		return "add keeper ";
	}


	@Override
	protected void doKeeperContainerOperation() {
		keeperContainerService.addOrStartKeeper(keeperInstanceMeta);		
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
				return "right keeper role command";
			}

			@Override
			protected void doExecute() throws Exception {
				
				KeeperRole keeperRole = (KeeperRole) new RoleCommand(keeperInstanceMeta.getKeeperMeta().getIp(), keeperInstanceMeta.getKeeperMeta().getPort()).execute().get();
				if(keeperRole.getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED){
					future().setSuccess(keeperRole);
				}else{
					future().setFailure(new KeeperMasterStateNotAsExpectedException(keeperInstanceMeta.getKeeperMeta(), keeperRole, MASTER_STATE.REDIS_REPL_CONNECTED));
				}
			}
			@Override
			protected void doReset() {
				
			}
		};
	}

	@Override
	protected void doReset() {
		
	}
}
