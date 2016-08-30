package com.ctrip.xpipe.redis.console.daltransaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.dal.jdbc.mapping.TableProviderManager;

import com.ctrip.xpipe.redis.console.daltransaction.XpipeDalTransactionManager;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
/**
 * @author shyin
 *
 * Aug 30, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class ConcurrentDalTransactionTest {
	
	public void doNothingInside() {
	}
	
	public void doNothingOutside() {
	}
	
	class TransactionRunnable implements Runnable{
		@Mock
		private TableProviderManager m_tableProviderManager;
		@Mock
		private DataSourceManager m_dataSourceManager;
		@InjectMocks
		private XpipeDalTransactionManager dalTM;
		
		@Override
		public void run() {
			DataSource ds = mock(DataSource.class);
			try {
				when(ds.getConnection()).thenReturn(mock(Connection.class));
			} catch (SQLException e) {
				
			}
			when(m_dataSourceManager.getDataSource("")).thenReturn(ds);
			
			assertEquals(dalTM.getThreadLocalTransactionInfo().get().getRecursiveLayer(),0);
			dalTM.startTransaction("");
			assertEquals(dalTM.getThreadLocalTransactionInfo().get().getRecursiveLayer(),1);
			doNothingOutside();
			dalTM.startTransaction("");
			assertEquals(dalTM.getThreadLocalTransactionInfo().get().getRecursiveLayer(),2);
			doNothingInside();
			dalTM.commitTransaction();
			assertEquals(dalTM.getThreadLocalTransactionInfo().get().getRecursiveLayer(),1);
			dalTM.commitTransaction();
			assertEquals(dalTM.getThreadLocalTransactionInfo().get().getRecursiveLayer(),0);
		}
		
	}
	
	@Test
	public void testConcurrentDalTransactionTest() {
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(100);
		for(int cnt = 0 ; cnt != 100 ; ++cnt) {
			fixedThreadPool.submit(new TransactionRunnable());
		}
	}
	
}
