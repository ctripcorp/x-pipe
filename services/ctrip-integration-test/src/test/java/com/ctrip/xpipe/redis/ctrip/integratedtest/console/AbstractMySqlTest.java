package com.ctrip.xpipe.redis.ctrip.integratedtest.console;

import com.ctrip.xpipe.redis.console.AbstractConsoleDbTest;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

/**
 * @author lishanglin
 * date 2021/4/21
 */
public class AbstractMySqlTest extends AbstractConsoleDbTest {

    public static final String MYSQL_ROOT_USER = "root";
    public static final String MYSQL_ROOT_PASSWORD = "";
    public static final String MYSQL_TEST_DATABASE = "fxxpipedb";
    public static final String CONTAINER_TZ = "Asia/Shanghai";

    public static final String TABLE_STRUCTURE = "sql/mysql/xpipedemodbtables.sql";
    public static final String TABLE_DATA = "sql/mysql/xpipedemodbinitdata.sql";

    protected static final boolean USE_EMBED_MYSQL = Boolean.parseBoolean(System.getProperty("xpipe.mysql.embed", "true"));

    private static MySQLContainer mysqlContainer = (MySQLContainer) new MySQLContainer(DockerImageName.parse("mysql:5.7.23"))
            .withDatabaseName(MYSQL_TEST_DATABASE)
            .withUsername(MYSQL_ROOT_USER)
            .withPassword(MYSQL_ROOT_PASSWORD)
            .withUrlParam("serverTimezone", CONTAINER_TZ)
            .withEnv("TZ", CONTAINER_TZ)
            .withEnv("MYSQL_ROOT_HOST", "%")
            .withEnv("MYSQL_ROOT_PASSWORD", MYSQL_ROOT_PASSWORD)
            .withCommand("mysqld --lower_case_table_names=1")
            .withCreateContainerCmdModifier(cmd -> {
                        ((CreateContainerCmd) cmd).getHostConfig().withPortBindings(new PortBinding(Ports.Binding.bindPort(3306), new ExposedPort(3306)));
                    }
            )
            .withExposedPorts(3306).withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("MySQL:3306")));

    @BeforeClass
    public static void setupAbstractMySqLContainerTest() throws Exception {
        if (USE_EMBED_MYSQL) mysqlContainer.start();
    }

    @AfterClass
    public static void afterAbstractMySqLContainerTest() {
        if (USE_EMBED_MYSQL) mysqlContainer.close();
    }

}
