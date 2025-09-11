package com.ctrip.xpipe.redis.console.healthcheck.nonredis.dbvariables;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceDescriptor;
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.dal.jdbc.datasource.JdbcDataSourceDescriptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class DBVariablesCheckTest extends AbstractTest {

    private DBVariablesCheck dbVariablesCheck;

    @Mock
    private DataSourceManager dataSourceManager;

    @Mock
    private VariableChecker checker;

    @Mock
    private ConsoleConfig consoleConfig;

    @Mock
    private DataSource dataSource;

    private static final Pattern idPattern = Pattern.compile("^single:[0-9.]+:[0-9]+$");

    private static final String userName = "testuser";
    private static final String password = "testpass";
    private static final String originDbUrl = "jdbc:mysql://127.0.0.1:55777, 127.0.0.2/fxxpipe?a=1&b=2";
    private static final List<String> targetDbUrls = Arrays.asList("jdbc:mysql://127.0.0.1:55777", "jdbc:mysql://127.0.0.2:3306");

    @Before
    public void setupDBVariablesCheckTest() {
        dbVariablesCheck = new DBVariablesCheck();

        dbVariablesCheck.setDataSourceManager(dataSourceManager);
        dbVariablesCheck.setVariableCheckers(Collections.singletonList(checker));
        dbVariablesCheck.setConsoleConfig(consoleConfig);

        Mockito.when(consoleConfig.getVariablesCheckDataSources()).thenReturn(Collections.singleton("fxxpipe"));
    }

    @Test
    @Ignore
    public void testDoCheck() {
        Mockito.when(dataSourceManager.getDataSource("fxxpipe")).thenReturn(dataSource);
        Mockito.when(dataSource.getDescriptor()).thenReturn(mockDataSourceDescriptor());

        AtomicInteger checkCnt = new AtomicInteger(0);
        Mockito.doAnswer(invocation -> {
            DataSource targetDatasource = invocation.getArgument(0, DataSource.class);
            logger.info("[testDoCheck] check datasource {}", targetDatasource.getDescriptor());

            Matcher matcher = idPattern.matcher(targetDatasource.getDescriptor().getId());
            Assert.assertTrue(matcher.find());

            Assert.assertEquals(userName, targetDatasource.getDescriptor().getProperty("user", null));
            Assert.assertEquals(password, targetDatasource.getDescriptor().getProperty("password", null));

            String datasourceUrl = targetDatasource.getDescriptor().getProperty("url", null);
            Assert.assertTrue(targetDbUrls.contains(datasourceUrl));

            checkCnt.incrementAndGet();
            return null;
        }).when(checker).check(any());

        dbVariablesCheck.doAction();
        Assert.assertEquals(2, checkCnt.get());
    }

    @Test
    public void testRefreshInterestedDataSources() {
        Assert.assertEquals(0, dbVariablesCheck.getInterestedDataSources().size());

        dbVariablesCheck.refreshInterestedDataSources();
        Assert.assertEquals(1, dbVariablesCheck.getInterestedDataSources().size());

        Mockito.when(consoleConfig.getVariablesCheckDataSources()).thenReturn(Collections.emptySet());
        dbVariablesCheck.refreshInterestedDataSources();
        Assert.assertEquals(0, dbVariablesCheck.getInterestedDataSources().size());
    }

    private DataSourceDescriptor mockDataSourceDescriptor() {
        JdbcDataSourceDescriptor descriptor = new JdbcDataSourceDescriptor();
        descriptor.setId("fxxpipe");
        descriptor.setType("xpipe");
        descriptor.setProperty("driver", "com.mysql.jdbc.Driver");
        descriptor.setProperty("user", userName);
        descriptor.setProperty("password", password);
        descriptor.setProperty("url", originDbUrl);

        return descriptor;
    }

}
