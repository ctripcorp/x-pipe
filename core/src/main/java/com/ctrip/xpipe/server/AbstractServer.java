package com.ctrip.xpipe.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.api.server.Server;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午3:27:08
 */
public abstract class AbstractServer implements Server{

	protected Logger logger = LogManager.getLogger(getClass());
	
	
	@Override
	public void run() {

			doRun();
	}


	protected abstract void doRun();
}
