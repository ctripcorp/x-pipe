package com.ctrip.xpipe.redis.ctrip.integratedtest.console;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author lishanglin
 * date 2021/4/21
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = AbstractConsoleIntegrationTest.ConsoleTestConfig.class)
public class AbstractCtripConsoleIntegrationTest extends AbstractCtripTest {
}
