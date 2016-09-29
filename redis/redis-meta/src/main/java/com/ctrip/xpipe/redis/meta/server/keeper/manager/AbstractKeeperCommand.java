package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.CommandRetryWrapper;
import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerErrorCode;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerException;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.retry.RetryDelay;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
public abstract class AbstractKeeperCommand<V> extends AbstractCommand<V>{
	
	protected final KeeperContainerService keeperContainerService;
	protected final KeeperTransMeta keeperTransMeta;
	protected int  timeoutMilli;
	protected int  checkIntervalMilli = 1000;
	
	public AbstractKeeperCommand(KeeperContainerService keeperContainerService, KeeperTransMeta keeperTransMeta, int timeoutMilli, int checkIntervalMilli) {
		this.keeperContainerService = keeperContainerService;
		this.keeperTransMeta = keeperTransMeta;
		this.timeoutMilli = timeoutMilli;
		this.checkIntervalMilli = checkIntervalMilli;
	}

	@Override
	protected void doExecute() throws Exception {
		
		logger.info("[doExecute]{}", this);
		doOpetation();
	}

	@SuppressWarnings("unchecked")
	private void doOpetation() {
		try{
			doKeeperContainerOperation();
			checkUntilStateOk();
		}catch(KeeperContainerException e){
			ErrorMessage<KeeperContainerErrorCode>  error = (ErrorMessage<KeeperContainerErrorCode>) e.getErrorMessage();
			if(error != null && isSuccess(error)){
				checkUntilStateOk();
			}else{
				future().setFailure(e);
			}
		}catch(Exception e){
			future().setFailure(e);
		}
	}

	protected void checkUntilStateOk(){
		
		CommandRetryWrapper.buildTimeoutRetry(timeoutMilli, 
				createRetryPolicy(), 
				createCheckStateCommand()).execute().addListener(new CommandFutureListener<V>() {

			@Override
			public void operationComplete(CommandFuture<V> commandFuture) throws Exception {
				
				if(commandFuture.isSuccess()){
					logger.info("[checkUntilStateOk][ok]{}",this);
					future().setSuccess(commandFuture.get());
				}else{
					logger.info("[checkUntilStateOk][fail]{}",this);
					future().setFailure(commandFuture.cause());
				}
			}
		});
	}

	protected RetryPolicy createRetryPolicy() {
		return new RetryDelay(checkIntervalMilli); 
	}

	protected abstract Command<V> createCheckStateCommand();

	protected abstract boolean isSuccess(ErrorMessage<KeeperContainerErrorCode> error);

	protected abstract void doKeeperContainerOperation();
	
	
	@Override
	public String getName() {
		return String.format("[%s(%s:%d)]", getClass().getSimpleName(), keeperTransMeta.getKeeperMeta().getIp(), keeperTransMeta.getKeeperMeta().getPort());
	}
}


