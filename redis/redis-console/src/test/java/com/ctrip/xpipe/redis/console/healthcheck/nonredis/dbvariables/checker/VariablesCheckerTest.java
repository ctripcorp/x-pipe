package com.ctrip.xpipe.redis.console.healthcheck.nonredis.dbvariables.checker;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceDescriptor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.mockito.ArgumentMatchers.*;

@RunWith(MockitoJUnitRunner.class)
public class VariablesCheckerTest extends AbstractTest {

    @Mock
    protected AlertManager alertManager;

    @Mock
    protected DataSource dataSource;

    @Mock
    protected Connection connection;

    @Mock
    protected PreparedStatement statement;

    @Mock
    protected ResultSet resultSet;

    @Before
    public void setupExitActionCheckerTest() throws Exception {
        Mockito.when(dataSource.getConnection()).thenReturn(connection);
        Mockito.when(dataSource.getDescriptor()).thenReturn(Mockito.mock(DataSourceDescriptor.class));
        Mockito.when(connection.prepareStatement(anyString(), anyInt(), anyInt()))
                .thenReturn(statement);
        Mockito.when(statement.executeQuery()).thenReturn(resultSet);
        Mockito.when(resultSet.first()).thenReturn(true);
    }

    @Test
    public void testExitActionCheck() throws Exception {
        ExitActionChecker checker = new ExitActionChecker(alertManager);

        Mockito.when(resultSet.getObject(anyString())).thenReturn("ABORT_SERVER");
        checker.check(dataSource);
        Mockito.verify(alertManager, Mockito.times(0)).alert(
                anyString(), anyString(), any(), any(), anyString());

        Mockito.when(resultSet.getObject(anyString())).thenReturn("READ_ONLY");
        checker.check(dataSource);
        Mockito.verify(alertManager, Mockito.times(1)).alert(
                anyString(), anyString(), any(), any(), anyString());

        Mockito.when(resultSet.getObject(anyString())).thenReturn(null);
        checker.check(dataSource);
        Mockito.verify(alertManager, Mockito.times(2)).alert(
                anyString(), anyString(), any(), any(), anyString());
    }

    @Test
    public void testUnreachableMajorityTimeoutCheck() throws Exception {
        UnreachableMajorityTimeoutChecker checker = new UnreachableMajorityTimeoutChecker(alertManager);

        Mockito.when(resultSet.getObject(anyString())).thenReturn(null);
        checker.check(dataSource);
        Mockito.verify(alertManager, Mockito.times(1)).alert(
                anyString(), anyString(), any(), any(), anyString());

        Mockito.when(resultSet.getObject(anyString())).thenReturn(0);
        checker.check(dataSource);
        Mockito.verify(alertManager, Mockito.times(2)).alert(
                anyString(), anyString(), any(), any(), anyString());

        Mockito.when(resultSet.getObject(anyString())).thenReturn(10);
        checker.check(dataSource);
        Mockito.verify(alertManager, Mockito.times(2)).alert(
                anyString(), anyString(), any(), any(), anyString());
    }

}
