package com.ctrip.xpipe.lifecycle;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.ctrip.xpipe.api.lifecycle.Disposable;
import com.ctrip.xpipe.api.lifecycle.Initializable;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午4:59:41
 */
public abstract class AbstractLifecycle implements Lifecycle{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private String phaseName = null;
	
	@Override
	public void initialize() throws Exception {
		try{
			if(logger.isInfoEnabled()){
				logger.info("[initialize]" + this);
			}
			this.phaseName = Initializable.PHASE_NAME_BEGIN;
			doInitialize();
		}finally{
			this.phaseName = Initializable.PHASE_NAME_END;
		}
	}
	
	protected void doInitialize() throws Exception{
		
	}

	@Override
	public void start() throws Exception {
		try{
			if(logger.isInfoEnabled()){
				logger.info("[start]" + this);
			}
			this.phaseName = Startable.PHASE_NAME_BEGIN;
			doStart();
		}finally{
			this.phaseName = Startable.PHASE_NAME_BEGIN;
		}
	}
	
	protected void doStart() throws Exception{
		
	}

	@Override
	public void stop() throws Exception {
		try{
			if(logger.isInfoEnabled()){
				logger.info("[stop]" + this);
			}
			this.phaseName = Stoppable.PHASE_NAME_BEGIN;
			doStop();
		}finally{
			this.phaseName = Stoppable.PHASE_NAME_END;
		}
	}

	protected void doStop() throws Exception{
		
	}

	@Override
	public void dispose() throws Exception {
		try{
			if(logger.isInfoEnabled()){
				logger.info("[dispose]" + this);
			}
			this.phaseName = Disposable.PHASE_NAME_BEGIN;
			doDispose();
		}finally{
			this.phaseName = Disposable.PHASE_NAME_BEGIN;
		}
	}

	protected void doDispose() throws Exception {
		
	}

	protected String getPhaseName(){
		
		return this.phaseName;
	}
}
