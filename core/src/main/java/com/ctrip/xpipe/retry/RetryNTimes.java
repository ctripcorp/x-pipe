package com.ctrip.xpipe.retry;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.ctrip.xpipe.api.retry.RetryType;
import com.ctrip.xpipe.api.retry.RetryWait;

/**
 * @author wenchao.meng
 *
 * Jul 9, 2016
 */
public class RetryNTimes extends AbstractRetryTemplate{
	
	private int n;
	private RetryWait retryWait;


	public RetryNTimes(int n, int delayBaseMilli) {
		this(n, new RetryDelay(delayBaseMilli));
	}

	public RetryNTimes(int n) {
		this(n, new NoWaitRetry());
	}

	public RetryNTimes(int n, RetryWait retryWait) {
		this.n = n;
		this.retryWait = retryWait;
	}

	@Override
	public boolean execute(Callable<RetryType> action){
		
		for(int i=0;i<=n;i++){
			try{
				if(i >= 1){
					int wait = retryWait.retryWaitMilli();
					logger.info("[execute][retry]{}, {}, {}", i, wait, action);
					try{
						TimeUnit.MILLISECONDS.wait(wait);
					}catch(Exception e){
						//noop
					}
				}
				RetryType type = action.call();
				switch(type){
					case SUCCESS:
						return true;
					case FAIL_PASS:
						break;
					default:
						continue;
				}
			}catch(Exception e){
				logger.error("[execute]" + action, e);
			}finally{
			}
		}

		return false;
	}
}
