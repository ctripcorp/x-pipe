package com.ctrip.xpipe.redis.console;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.spring.AbstractProfile;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = App.class)
public class AbstractConsoleIntegrationTest extends AbstractConsoleTest {
	
	@Before
	public void setUp() {
		System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_TEST);
		System.setProperty("FXXPIPE_HOME", "src/test/resources");
	}
	
	@After
	public void tearDown() {
		ContainerLoader.getDefaultContainer().dispose();
	}
}
