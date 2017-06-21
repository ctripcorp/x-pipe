package com.ctrip.xpipe.redis.console.simple;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import org.h2.tools.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;

/**
 * @author wenchao.meng
 *         <p>
 *         May 11, 2017
 */
public class H2Test extends AbstractConsoleTest{

    private Connection conn;
    private String dbName = "testdb";
    private String tableName = "person";
    private int h2Port = 9123;

    @Before
    public void beforeH2Test() throws SQLException, ClassNotFoundException {

        Class.forName("org.h2.Driver");
        conn = createConnection();
        startH2Server();
    }

    private Connection createConnection() throws SQLException {
        return DriverManager.
                getConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MV_STORE=FALSE", "sa", "");
    }

    @Test
    public void testH2() throws ClassNotFoundException, SQLException {

        // add application code here

        String sql = String.format("create table %s (id int, name varchar(30))", tableName);
        executeSql(sql);

        sql = String.format("insert into %s(id, name) values (%d, '%s')", tableName, 1, randomString(10));
        executeSql(sql);

        sql = String.format("select * from %s", tableName);
        executeQuery(sql);

        logger.info("[close and query]");
        conn.close();

        conn = createConnection();
        executeQuery(sql);

    }

    protected void startH2Server() throws SQLException {

        Server tcpServer = Server.createTcpServer("-tcpPort", String.valueOf(h2Port), "-tcpAllowOthers");
        tcpServer.start();
    }


    private void executeQuery(String query) {

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                logger.info("id:{}, name:{}", id, name);
            }
        } catch (SQLException e ) {
            logger.error("[executeQuery]", e);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    logger.error("[executeQuery]", e);
                }
            }
        }
    }

    private void executeSql(String sql) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        preparedStatement.execute();
    }


    @After
    public void afterH2Test() throws SQLException, IOException {
        conn.close();

        waitForAnyKey();
    }

}
