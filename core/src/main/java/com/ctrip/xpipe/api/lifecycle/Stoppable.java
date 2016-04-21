package com.ctrip.xpipe.api.lifecycle;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午4:57:05
 */
public interface Stoppable {

	void stop() throws Exception;
}
