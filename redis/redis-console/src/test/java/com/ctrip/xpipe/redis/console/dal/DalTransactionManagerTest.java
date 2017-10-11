package com.ctrip.xpipe.redis.console.dal;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.dal.XpipeDalTransactionManager.TransactionInfo;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.dal.jdbc.DalRuntimeException;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.dal.jdbc.mapping.TableProviderManager;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @author shyin
 *
 * Aug 29, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DalTransactionManagerTest extends AbstractConsoleTest{
	@Mock
	private TableProviderManager m_tableProviderManager;
	@Mock
	private DataSourceManager m_dataSourceManager;
	@InjectMocks
	private XpipeDalTransactionManager dalTM;
	
	@Rule
	public ExpectedException thrown = ExpectedException.none();
	
	@Before
	public void setUp() {
		TransactionInfo trx = new TransactionInfo();
		dalTM.getThreadLocalTransactionInfo().set(trx);
	}
	
	@Test
	public void testGetConnectionExceptionForStartTransaction() throws SQLException {
		DataSource ds = mock(DataSource.class);
		Connection conn = mock(Connection.class);
		when(ds.getConnection()).thenReturn(conn);
		when(m_dataSourceManager.getDataSource("")).thenReturn(ds);
		doThrow(new SQLException()).when(conn).setAutoCommit(false);
		
		thrown.expect(DalRuntimeException.class);
		dalTM.startTransaction("");
	}
	
	@Test
	public void testInvalidRecursiveLayerForStartTransaction() {
		dalTM.getThreadLocalTransactionInfo().get().decrRecursiveLayer();
		thrown.expectMessage("Cannot start transaction.");
		dalTM.startTransaction("");
	}
	
	@Test
	public void testStartTransaction() throws SQLException {
		DataSource ds = mock(DataSource.class);
		when(ds.getConnection()).thenReturn(mock(Connection.class));
		when(m_dataSourceManager.getDataSource("")).thenReturn(ds);
		
		assertEquals(dalTM.getThreadLocalTransactionInfo().get().isInTransaction(),false);
		
		dalTM.startTransaction("");
		assertEquals(dalTM.getThreadLocalTransactionInfo().get().getRecursiveLayer(),1);
		assertEquals(dalTM.getThreadLocalTransactionInfo().get().isInTransaction(),true);
		
		dalTM.startTransaction("");
		assertEquals(dalTM.getThreadLocalTransactionInfo().get().getRecursiveLayer(),2);
		
	}
	
	@Test
	public void testInTransactionExceptionForCommitTransaction() {
		dalTM.getThreadLocalTransactionInfo().get().setInTransaction(false);
		thrown.expectMessage("There is no active transaction open, can't commit");
		dalTM.commitTransaction();
	}
	
	@Test
	public void testInvalidRecursiveLayerForCommitTransaction() {
		dalTM.getThreadLocalTransactionInfo().get().setInTransaction(true);
		dalTM.getThreadLocalTransactionInfo().get().decrRecursiveLayer();
		thrown.expectMessage("Invalid transaction commit");
		dalTM.commitTransaction();
	}
	
	@Test
	public void testCommitTransaction() throws SQLException {
		dalTM.getThreadLocalTransactionInfo().get().setInTransaction(true);
		// set recursive layer to 2
		dalTM.getThreadLocalTransactionInfo().get().incrRecursiveLayer();
		dalTM.getThreadLocalTransactionInfo().get().incrRecursiveLayer();
		dalTM.getThreadLocalTransactionInfo().get().setConnection(mock(Connection.class));
		
		dalTM.commitTransaction();
		assertEquals(dalTM.getThreadLocalTransactionInfo().get().getRecursiveLayer(),1);
		
		dalTM.commitTransaction();
		assertEquals(dalTM.getThreadLocalTransactionInfo().get().getRecursiveLayer(),0);
		assertEquals(dalTM.getThreadLocalTransactionInfo().get().isInTransaction(),false);
		
		// set recursive layer back to 1
		dalTM.getThreadLocalTransactionInfo().get().setInTransaction(true);
		dalTM.getThreadLocalTransactionInfo().get().incrRecursiveLayer();
		dalTM.getThreadLocalTransactionInfo().get().setConnection(mock(Connection.class));
		doThrow(new SQLException()).when(dalTM.getThreadLocalTransactionInfo().get().getConnection()).commit();
		thrown.expect(DalRuntimeException.class);
		dalTM.commitTransaction();
	}
	
	@Test
	public void testInTransactionExceptionForRollbackTransaction() {
		dalTM.getThreadLocalTransactionInfo().get().setInTransaction(false);
		thrown.expectMessage("There is no active transaction open, can't tryRollback");
		dalTM.rollbackTransaction();
	}
	
	@Test
	public void testInvalidRecursiveLayerForRollbackTransaction() {
		dalTM.getThreadLocalTransactionInfo().get().setInTransaction(true);
		dalTM.getThreadLocalTransactionInfo().get().decrRecursiveLayer();
		thrown.expectMessage("Invalid transaction tryRollback");
		dalTM.rollbackTransaction();
	}
	
	@Test
	public void testRollbackTransaction() throws SQLException {
		dalTM.getThreadLocalTransactionInfo().get().setInTransaction(true);
		// set recursive layer to 2
		dalTM.getThreadLocalTransactionInfo().get().incrRecursiveLayer();
		dalTM.getThreadLocalTransactionInfo().get().incrRecursiveLayer();
		dalTM.getThreadLocalTransactionInfo().get().setConnection(mock(Connection.class));
		
		dalTM.rollbackTransaction();
		assertEquals(dalTM.getThreadLocalTransactionInfo().get().getRecursiveLayer(),1);
		
		dalTM.rollbackTransaction();
		assertEquals(dalTM.getThreadLocalTransactionInfo().get().getRecursiveLayer(),0);
		assertEquals(dalTM.getThreadLocalTransactionInfo().get().isInTransaction(),false);
		
		// set recursive layer back to 1
		dalTM.getThreadLocalTransactionInfo().get().setInTransaction(true);
		dalTM.getThreadLocalTransactionInfo().get().incrRecursiveLayer();
		dalTM.getThreadLocalTransactionInfo().get().setConnection(mock(Connection.class));
		doThrow(new SQLException()).when(dalTM.getThreadLocalTransactionInfo().get().getConnection()).rollback();
		thrown.expect(DalRuntimeException.class);
		dalTM.rollbackTransaction();
	}
}
