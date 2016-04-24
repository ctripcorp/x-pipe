package com.ctrip.xpipe.lifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午4:59:41
 */
public abstract class AbstractLifecycle implements Lifecycle{
	
	protected Logger logger = LogManager.getLogger(getClass());
	
	@Override
	public void initialize() throws Exception {
		
		if(logger.isInfoEnabled()){
			logger.info("[initialize]" + this);
		}
		doInitialize();
	}
	
	protected void doInitialize() throws Exception{
		
	}

	@Override
	public void start() throws Exception {
		if(logger.isInfoEnabled()){
			logger.info("[start]" + this);
		}
		doStart();
	}
	
	protected void doStart() throws Exception{
		
	}

	@Override
	public void stop() throws Exception {
		if(logger.isInfoEnabled()){
			logger.info("[stop]" + this);
		}
		doStop();
	}

	protected void doStop() throws Exception{
		
	}

	@Override
	public void dispose() throws Exception {
		if(logger.isInfoEnabled()){
			logger.info("[dispose]" + this);
		}
		doDispose();
	}

	protected void doDispose() throws Exception {
		
	}
}
