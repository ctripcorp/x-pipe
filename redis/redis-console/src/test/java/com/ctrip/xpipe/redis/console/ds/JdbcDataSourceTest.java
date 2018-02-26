package com.ctrip.xpipe.redis.console.ds;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import org.codehaus.plexus.logging.AbstractLogger;
import org.codehaus.plexus.logging.Logger;
import org.junit.Test;
import org.unidal.dal.jdbc.datasource.JdbcDataSource;
import org.unidal.dal.jdbc.datasource.JdbcDataSourceDescriptor;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Feb 23, 2018
 */
public class JdbcDataSourceTest extends AbstractConsoleTest {

    @Test
    public void testDomain() throws IOException {

        String host = "test.domain";

        scheduled.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    logger.info("host: {}", InetAddress.getByName(host));
                    new Socket(host, 8080);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, 0, 5, TimeUnit.SECONDS);

        sleep(1000000);
    }


    @Test
    public void testGetConnection() {

        JdbcDataSource jdbcDataSource = newJdbcDataSource();

        scheduled.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {

                Connection connection = null;
                try {
                    logger.info("[connection][begin]");
                    connection = jdbcDataSource.getConnection();
                    logger.info("[connection][gotConnection]{}", connection);
                    executeQuery(connection);
                    logger.info("[connection][ end ]{}", connection);
                } catch (SQLException e) {
                    logger.error("[testGetConnection fail]", e);
                } finally {
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            logger.error("[close]" + connection, e);
                        }
                    }
                }
            }
        }, 0, 1, TimeUnit.SECONDS);

        sleep(1000000);
    }


    @Test
    public void test() throws SQLException {

        JdbcDataSource jdbcDataSource = newJdbcDataSource();

        while (true) {
            sleep(5000);
            Connection connection = null;
            try {
                logger.info("get Connection");
                connection = jdbcDataSource.getConnection();
                executeQuery(connection);
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
        }
    }

    private void executeQuery(Connection connection) {

        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select count(*) from test");
            while (resultSet.next()) {
                long count = resultSet.getLong(1);
                logger.info("count:{}", count);
            }
        } catch (SQLException e) {
            logger.error("[executeQuery fail]", e);
        }
    }

    private JdbcDataSource newJdbcDataSource() {

        JdbcDataSource jdbcDataSource = new JdbcDataSource();
        JdbcDataSourceDescriptor descriptor = new JdbcDataSourceDescriptor();
        descriptor.setProperty("url", "jdbc:mysql://localhost:3306/test");
        descriptor.setProperty("driver", "com.mysql.jdbc.Driver");
        descriptor.setProperty("user", "root");
        descriptor.setProperty("password", "root");
//        descriptor.setProperty("checkout-timeout", "5000");
        jdbcDataSource.enableLogging(new NullLogger(Logger.LEVEL_DEBUG, getTestName()));
        jdbcDataSource.initialize(descriptor);

        return jdbcDataSource;
    }


    public static class NullLogger extends AbstractLogger {

        public NullLogger(int threshold, String name) {
            super(threshold, name);
        }

        @Override
        public void debug(String message, Throwable throwable) {

        }

        @Override
        public void info(String message, Throwable throwable) {

        }

        @Override
        public void warn(String message, Throwable throwable) {

        }

        @Override
        public void error(String message, Throwable throwable) {

        }

        @Override
        public void fatalError(String message, Throwable throwable) {

        }

        @Override
        public Logger getChildLogger(String name) {
            return null;
        }
    }

}
