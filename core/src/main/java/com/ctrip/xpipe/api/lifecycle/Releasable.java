package com.ctrip.xpipe.api.lifecycle;

/**
 * @author wenchao.meng
 *
 * Jun 3, 2016
 */
public interface Releasable {

	void release() throws Exception;
}
