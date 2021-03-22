package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public abstract class AbstractConsoleTest extends AbstractCheckerTest {

    @Override
    protected CheckerConfig buildCheckerConfig() {
        return new DefaultConsoleConfig();
    }

}
