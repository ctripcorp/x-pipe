package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import java.util.List;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerErrorCode;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
public class DeleteKeeperCommand extends AbstractKeeperCommand<Void>{
	
	private CurrentMetaManager currentMetaManager;

	public DeleteKeeperCommand(CurrentMetaManager currentMetaManager, KeeperContainerService keeperContainerService, KeeperTransMeta keeperTransMeta,
			int timeoutMilli) {
		this(currentMetaManager, keeperContainerService, keeperTransMeta, timeoutMilli, 1000);
	}

	public DeleteKeeperCommand(CurrentMetaManager currentMetaManager, KeeperContainerService keeperContainerService, KeeperTransMeta keeperTransMeta,
			int timeoutMilli, int checkIntervalMilli) {
		super(keeperContainerService, keeperTransMeta, timeoutMilli, checkIntervalMilli);
		this.currentMetaManager = currentMetaManager;
	}


	@Override
	public String getName() {
		return "delete keeper ";
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
		return new AbstractCommand<Void>() {

			@Override
			public String getName() {
				return "[check keeper deleted]";
			}

			@Override
			protected void doExecute() throws Exception {
				
				List<KeeperMeta> surviveKeepers = currentMetaManager.getSurviveKeepers(keeperTransMeta.getClusterId(), keeperTransMeta.getShardId());
				for(KeeperMeta keeperSurvive : surviveKeepers){
					if(MetaUtils.same(keeperSurvive, keeperTransMeta.getKeeperMeta())){
						logger.info("[doExecute][keeper still alive]", keeperTransMeta.getKeeperMeta());
						future().setFailure(new DeleteKeeperStillAliveException(surviveKeepers, keeperTransMeta.getKeeperMeta()));
						return;
					}
				}
				future().setSuccess();
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
