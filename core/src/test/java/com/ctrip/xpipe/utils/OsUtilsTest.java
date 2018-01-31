package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Jan 29, 2018
 */
public class OsUtilsTest extends AbstractTest {

    @Test
    public void test(){

        int i = OsUtils.defaultMaxCoreThreadCount();
        logger.info("{}", i);

    }
}
