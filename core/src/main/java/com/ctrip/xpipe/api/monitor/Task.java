package com.ctrip.xpipe.api.monitor;

import java.util.Map;

/**
 * @author wenchao.meng
 *
 * Jan 3, 2017
 */
public interface Task<T> {

	void go() throws Exception;

	Map<String, T> getData();
}
