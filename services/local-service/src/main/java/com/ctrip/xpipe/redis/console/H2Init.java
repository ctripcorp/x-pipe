package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.utils.FileUtils;
import com.google.common.base.Joiner;
import org.apache.logging.log4j.util.Strings;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringApplicationRunListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.lookup.ContainerLoader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 31, 2017
 */
public class H2Init implements SpringApplicationRunListener {

    private Logger logger = LoggerFactory.getLogger(getClass());

    public static String DATA_SOURCE = "fxxpipe";

    public static final String DEMO_DATA = "init_data.sql";
    public static final String TABLE_STRUCTURE = "sql/h2/xpipedemodbtables.sql";
    public static final String TABLE_DATA = "sql/h2/xpipedemodbinitdata.sql";
    protected String[] dcNames = new String[]{"jq", "oy"};

    public H2Init(SpringApplication application, String[] args) {
        logger.info("{}", application);
        logger.info("{}", Joiner.on(",").join(args));
    }

//    @Override
    public void started(ConfigurableApplicationContext context) {
        //have to be sysout
        System.out.println("[started][execute sql]");
        try {
            setUpTestDataSource();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    private void setUpTestDataSource() throws ComponentLookupException, SQLException, IOException {

        DataSourceManager dsManager = ContainerLoader.getDefaultContainer().lookup(DataSourceManager.class);
        DataSource dataSource = null;
        try {
            dataSource = dsManager.getDataSource(DATA_SOURCE);
        } catch(Exception e) {
            logger.info("[setUpTestDataSource][ignore if it it not console]{}", e.getMessage());
            return;
        }

        String driver = dataSource.getDescriptor().getProperty("driver", null);
        if (driver != null && driver.equals("org.h2.Driver")) {
            executeSqlScript(FileUtils.readFileAsString(TABLE_STRUCTURE));
            executeSqlScript(FileUtils.readFileAsString(DEMO_DATA));

        } else {
            logger.info("[setUpTestDataSource][do not clean]{}", driver);
        }
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

//    @Override
    public void environmentPrepared(ConfigurableEnvironment environment) {

    }

    @Override
    public void contextPrepared(ConfigurableApplicationContext context) {

    }

    @Override
    public void contextLoaded(ConfigurableApplicationContext context) {

    }

}
