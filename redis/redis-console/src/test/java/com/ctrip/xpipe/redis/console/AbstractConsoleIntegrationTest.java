package com.ctrip.xpipe.redis.console;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.util.Strings;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.health.HealthChecker;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.FileUtils;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = App.class)
public abstract class AbstractConsoleIntegrationTest extends AbstractConsoleTest {
	public static String DATA_SOURCE = "fxxpipe";
	
	@BeforeClass
	public static void setUp() {
		System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_TEST);
		System.setProperty("spring.main.show_banner", "false");
		System.setProperty("FXXPIPE_HOME", "src/test/resources");
	}
	
	@Before
	public void before() throws ComponentLookupException, SQLException {
		setUpTestDataSource();
	}
	
	@After
	public void tearDown() {
		ContainerLoader.destroyDefaultContainer();
	}

	
	private void setUpTestDataSource() throws ComponentLookupException, SQLException {
		DataSourceManager dsManager = ContainerLoader.getDefaultContainer().lookup(DataSourceManager.class);
		
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = dsManager.getDataSource(DATA_SOURCE).getConnection();
			String prepareSql = prepareDatas();
			if(!Strings.isEmpty(prepareSql)) {
				for(String sql : prepareSql.split(";")) {
					logger.debug("[setup][data]{}",sql.trim());
					stmt = conn.prepareStatement(sql);
					stmt.executeUpdate();
				}
			}
			
		} catch (Exception ex) {
			logger.error("[SetUpTestDataSource][fail]:",ex);
		} finally {
			if(null != stmt) {
				stmt.close();
			}
			if (null != conn) {
				conn.close();
			}
		}
		
	}

	protected String prepareDatas() {
		return "";
	}
	
	public String prepareDatasFromFile(String path) throws IOException {
		InputStream ins = FileUtils.getFileInputStream(path);
		return IOUtils.toString(ins);
	}
}
