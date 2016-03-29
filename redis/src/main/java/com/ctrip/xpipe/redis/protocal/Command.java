package com.ctrip.xpipe.redis.protocal;

import java.io.IOException;

import com.ctrip.xpipe.exception.XpipeException;



/**
 * @author wenchao.meng
 *
 * 2016年3月24日 上午11:32:27
 */
public interface Command {
	
	String getName();
	
	void request() throws IOException, XpipeException;
	
}