package com.ctrip.xpipe.redis.console;


import org.junit.runner.RunWith;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = App.class)
public abstract class AbstractConsoleIntegrationTest extends AbstractConsoleH2DbTest{
}
