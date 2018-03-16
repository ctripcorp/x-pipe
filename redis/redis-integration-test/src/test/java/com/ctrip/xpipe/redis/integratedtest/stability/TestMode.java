package com.ctrip.xpipe.redis.integratedtest.stability;

import java.io.Closeable;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 13, 2018
 */
public interface TestMode extends Closeable {

    void test() throws Exception;

}
