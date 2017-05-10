package com.ctrip.xpipe.api.concurrent;

import java.util.concurrent.ExecutorService;

/**
 * @author wenchao.meng
 *         <p>
 *         May 10, 2017
 */
public interface ExecutorFactory {

    ExecutorService createExecutorService();

}
