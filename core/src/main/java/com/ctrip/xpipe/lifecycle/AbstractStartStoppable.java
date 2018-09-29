package com.ctrip.xpipe.lifecycle;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author wenchao.meng
 *
 * Sep 21, 2016
 */
public abstract class AbstractStartStoppable implements Startable, Stoppable{
	
	private AtomicBoolean isStarted = new AtomicBoolean(false);
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public void start() throws Exception {

		
		if(isStarted.compareAndSet(false, true)){
			logger.info("[start]{}", this);
			doStart();
		}else{
			logger.warn("[start][already started]");
		}
	}

	protected abstract void doStart() throws Exception;


	@Override
	public void stop() throws Exception {
		if(isStarted.compareAndSet(true, false)){
			logger.info("[stop]{}", this);
			doStop();
		}else{
			logger.warn("[stop][already stopped]");
		}
	}

	protected abstract void doStop() throws Exception;
	
	
	public boolean isStarted() {
		return isStarted.get();
	}
}
