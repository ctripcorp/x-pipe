package simpletest;

import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.component.composition.CycleDetectedInComponentGraphException;
import org.codehaus.plexus.component.repository.exception.ComponentLifecycleException;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.dal.jdbc.test.TestDataSourceManager;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;


public class PlexusContextTest extends AbstractConsoleTest {
	PlexusContainer container;
	
	@Before
	public void setUp() {
		container = ContainerLoader.getDefaultContainer();
		logger.info("Load Container:{}", container);
	}
	
	@Test
	public void releaseTest() throws ComponentLookupException, ComponentLifecycleException, CycleDetectedInComponentGraphException {
		DataSourceManager dsManager = container.lookup(DataSourceManager.class);
		Assert.assertNotNull(dsManager);
		container.addComponent(new TestDataSourceManager(), "org.unidal.dal.jdbc.datasource.DataSourceManager");
		Assert.assertTrue(container.lookup(DataSourceManager.class) instanceof TestDataSourceManager);
	}
}
