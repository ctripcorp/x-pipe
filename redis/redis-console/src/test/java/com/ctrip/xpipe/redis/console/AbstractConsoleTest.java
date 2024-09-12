package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.config.impl.CheckConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.CommonConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.ConsoleConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.DataCenterConfigBean;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import org.junit.BeforeClass;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public abstract class AbstractConsoleTest extends AbstractCheckerTest {
    
    @BeforeClass
    public static void beforeAbstractConsoleTest() {
        System.setProperty("DisableLoadProxyAgentJar", "true");
    }

    @Override
    protected CheckerConfig buildCheckerConfig() {

        return new DefaultConsoleConfig(new CheckConfigBean(FoundationService.DEFAULT),
                new ConsoleConfigBean(FoundationService.DEFAULT),
                new DataCenterConfigBean(),
                new CommonConfigBean());
    }

}
