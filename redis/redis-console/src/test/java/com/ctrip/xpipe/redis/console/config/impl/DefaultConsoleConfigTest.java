package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 15, 2017
 */
public class DefaultConsoleConfigTest extends AbstractConsoleTest{

    @Test
    public void test(){
        ConsoleConfig consoleConfig = new DefaultConsoleConfig();
        logger.info("{}", consoleConfig.alertClientConfigConsistent());

    }
}
