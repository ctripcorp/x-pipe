package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.redis.console.build.ComponentsConfigurator;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.FileUtils;
import com.ctrip.xpipe.utils.StringUtil;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.util.Strings;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.h2.tools.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.lookup.ContainerLoader;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 17, 2017
 */
public class AbstractConsoleH2DbTest extends AbstractConsoleTest {

    public static String DATA_SOURCE = "fxxpipe";

    public static final String TABLE_STRUCTURE = "sql/h2/xpipedemodbtables.sql";
    public static final String TABLE_DATA = "sql/h2/xpipedemodbinitdata.sql";
    protected String[] dcNames = new String[]{"jq", "oy"};

    @BeforeClass
    public static void setUp() {

        System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_TEST);
        System.setProperty("spring.main.show_banner", "false");

        String dataSourceLocation = System.getProperty(ComponentsConfigurator.KEY_XPIPE_LOCATION);
        if (StringUtil.isEmpty(dataSourceLocation)) {
            System.setProperty(ComponentsConfigurator.KEY_XPIPE_LOCATION, "src/test/resources");
        } else {
            Logger logger = LoggerFactory.getLogger("AbstractConsoleH2DbTest");
            logger.info("[setUp][dataSource location]{}", dataSourceLocation);
        }
    }

    @Before
    public void before() throws ComponentLookupException, SQLException, IOException {
//        ContainerLoader.getDefaultContainer().lookup(TransactionManager.class, "xpipe");
//        ContainerLoader.getDefaultContainer().lookup(DataSourceManager.class, "xpipe");
        setUpTestDataSource();
    }

    @After
    public void tearDown() throws ComponentLookupException {
//        Assert.assertFalse(ContainerLoader.getDefaultContainer().lookup(TransactionManager.class) instanceof DefaultTransactionManager);
//        Assert.assertTrue(ContainerLoader.getDefaultContainer().lookup(TransactionManager.class) instanceof XpipeDalTransactionManager);
//        Assert.assertTrue(ContainerLoader.getDefaultContainer().lookup(DataSourceManager.class) instanceof XPipeDataSourceManager);
        ContainerLoader.destroy();
    }

    private void setUpTestDataSource() throws ComponentLookupException, SQLException, IOException {

        DataSourceManager dsManager = ContainerLoader.getDefaultContainer().lookup(DataSourceManager.class);
        DataSource dataSource = dsManager.getDataSource(DATA_SOURCE);
        String driver = dataSource.getDescriptor().getProperty("driver", null);

        if (driver != null && driver.equals("org.h2.Driver")) {
            executeSqlScript(FileUtils.readFileAsString(TABLE_STRUCTURE));
            executeSqlScript(FileUtils.readFileAsString(TABLE_DATA));
        } else {
            logger.info("[setUpTestDataSource][do not clean]{}", driver);
        }

        executeSqlScript(prepareDatas());
    }

    protected void executeSqlScript(String prepareSql) throws ComponentLookupException, SQLException {

        DataSourceManager dsManager = ContainerLoader.getDefaultContainer().lookup(DataSourceManager.class);

        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = dsManager.getDataSource(DATA_SOURCE).getConnection();
            conn.setAutoCommit(false);
            if (!Strings.isEmpty(prepareSql)) {
                for (String sql : prepareSql.split(";")) {
                    logger.debug("[setup][data]{}", sql.trim());
                    stmt = conn.prepareStatement(sql);
                    stmt.executeUpdate();
                }
            }
            conn.commit();

        } catch (Exception ex) {
            logger.error("[SetUpTestDataSource][fail]:", ex);
            if (null != conn) {
                conn.rollback();
            }
        } finally {
            if (null != stmt) {
                stmt.close();
            }
            if (null != conn) {
                conn.setAutoCommit(true);
                conn.close();
            }
        }
    }


    protected final String KEY_H2_PORT = "h2Port";
    private Server h2Server;

    protected void startH2Server() throws SQLException {

        int h2Port = Integer.parseInt(System.getProperty(KEY_H2_PORT, "9123"));
        h2Server = Server.createTcpServer("-tcpPort", String.valueOf(h2Port), "-tcpAllowOthers");
        h2Server.start();
//		new Console().runTool();
    }


    protected String prepareDatas() throws IOException {
        return "";
    }

    public String prepareDatasFromFile(String path) throws IOException {
        InputStream ins = FileUtils.getFileInputStream(path);
        return IOUtils.toString(ins);
    }


}
