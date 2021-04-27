package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.utils.FileUtils;
import com.ctrip.xpipe.utils.StringUtil;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.util.Strings;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.junit.After;
import org.junit.Before;
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.lookup.ContainerLoader;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author lishanglin
 * date 2021/4/21
 */
public class AbstractConsoleDbTest extends AbstractConsoleTest {

    public static String DATA_SOURCE = "fxxpipe";

    @Before
    public void before() throws ComponentLookupException, SQLException, IOException {
        setUpTestDataSource();
    }

    @After
    public void tearDown() {
        ContainerLoader.destroy();
    }

    protected void setUpTestDataSource() throws ComponentLookupException, SQLException, IOException {
        executeSqlScript(prepareDatas());
    }

    protected void executeSqlScript(String prepareSql) throws ComponentLookupException, SQLException {

        if (StringUtil.isEmpty(prepareSql)) {
            return;
        }

        DataSourceManager dsManager = ContainerLoader.getDefaultContainer().lookup(DataSourceManager.class);

        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = dsManager.getDataSource(DATA_SOURCE).getConnection();
            conn.setAutoCommit(false);
            if (!Strings.isEmpty(prepareSql)) {
                for (String sql : prepareSql.split(";")) {
                    String executeSql = sql.trim();
                    if (StringUtil.isEmpty(executeSql)) continue;

                    logger.debug("[setup][data]{}", executeSql);
                    stmt = conn.prepareStatement(executeSql);
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

    protected String prepareDatas() throws IOException {
        return "";
    }

    public static String prepareDatasFromFile(String path) throws IOException {
        InputStream ins = FileUtils.getFileInputStream(path);
        return IOUtils.toString(ins);
    }

}
