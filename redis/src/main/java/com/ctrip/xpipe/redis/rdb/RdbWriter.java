package com.ctrip.xpipe.redis.rdb;

import java.io.OutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午4:58:43
 */
public abstract class RdbWriter extends OutputStream{
	
	protected Logger logger = LogManager.getLogger(getClass());

	public void beginWrite(){
		if(logger.isInfoEnabled()){
			logger.info("[beginWrite]" + this);
		}
		doBeginWrite();
	}
	
	protected abstract void doBeginWrite();

	public void endWrite(){
		if(logger.isInfoEnabled()){
			logger.info("[endWrite]" + this);
		}
		doEndWrite();
	}

	protected abstract void doEndWrite();

}
