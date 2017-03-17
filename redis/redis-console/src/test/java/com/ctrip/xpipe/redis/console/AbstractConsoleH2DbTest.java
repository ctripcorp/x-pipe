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
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.build.ComponentsConfigurator;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.FileUtils;

/**
 * @author wenchao.meng
 *
 * Mar 17, 2017
 */
public class AbstractConsoleH2DbTest extends AbstractConsoleTest{
	
	public static String DATA_SOURCE = "fxxpipe";
	
	public static final String TABLE_STRUCTURE = "sql/h2/xpipedemodbtables.sql";
	public static final String TABLE_DATA = "sql/h2/xpipedemodbinitdata.sql";
	
	
	@BeforeClass
	public static void setUp() {
		System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_TEST);
		System.setProperty("spring.main.show_banner", "false");
		System.setProperty(ComponentsConfigurator.KEY_XPIPE_LOCATION, "src/test/resources");
		System.setProperty("cat.client.enabled", "false");
	}
	
	@Before
	public void before() throws ComponentLookupException, SQLException, IOException {
		setUpTestDataSource();
	}
	
	@After
	public void tearDown() {
		ContainerLoader.destroyDefaultContainer();
	}

	private void setUpTestDataSource() throws ComponentLookupException, SQLException, IOException {
		
		executeSqlScript(FileUtils.readFileAsString(TABLE_STRUCTURE));
		executeSqlScript(FileUtils.readFileAsString(TABLE_DATA));
		executeSqlScript(prepareDatas());
	}

	private void executeSqlScript(String prepareSql) throws ComponentLookupException, SQLException {
		
		DataSourceManager dsManager = ContainerLoader.getDefaultContainer().lookup(DataSourceManager.class);
		
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = dsManager.getDataSource(DATA_SOURCE).getConnection();
			conn.setAutoCommit(false);
			if(!Strings.isEmpty(prepareSql)) {
				for(String sql : prepareSql.split(";")) {
					logger.debug("[setup][data]{}",sql.trim());
					stmt = conn.prepareStatement(sql);
					stmt.executeUpdate();
				}
			}
			conn.commit();
			
		} catch (Exception ex) {
			logger.error("[SetUpTestDataSource][fail]:",ex);
			if(null != conn) {
				conn.rollback();
			}
		} finally {
			if(null != stmt) {
				stmt.close();
			}
			if (null != conn) {
				conn.setAutoCommit(true);
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
