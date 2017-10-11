package com.ctrip.xpipe.redis.console.dal;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.dal.jdbc.mapping.TableProviderManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
/**
 * @author shyin
 *
 * Aug 30, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class ConcurrentDalTransactionTest {

	@Mock
	private TableProviderManager m_tableProviderManager;
	@Mock
	private DataSourceManager m_dataSourceManager;
	@InjectMocks
	private XpipeDalTransactionManager dalTM;
	
	
	@Test
	public void testConcurrentDalTransactionTest() throws InterruptedException, ExecutionException {
		int threadCnt = 100;
		
		Callable<Integer> task = new TransactionCallable();
		List<Callable<Integer>> tasks = Collections.nCopies(threadCnt, task);
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(threadCnt);
		List<Future<Integer>> futures = fixedThreadPool.invokeAll(tasks);
		
		List<Integer> results = new ArrayList<Integer>(futures.size());
		// check for exceptions 
		for(Future<Integer> future : futures) {
			results.add(future.get());
		}
		
		// validate results
		assertEquals(threadCnt, results.size());
		for(int result : results) {
			assertEquals(result, Integer.MAX_VALUE);
		}
	}
	
	class TransactionCallable implements Callable<Integer>{
		public final Integer SUCCESS_TAG = Integer.MAX_VALUE;

		@Override
		public Integer call() throws Exception {
			Integer result = 0;
			
			DataSource ds = mock(DataSource.class);
			try {
				when(ds.getConnection()).thenReturn(mock(Connection.class));
			} catch (SQLException e) {
				
			}
			when(m_dataSourceManager.getDataSource("")).thenReturn(ds);
			
			validateRecursiveLayer(dalTM.getThreadLocalTransactionInfo().get().getRecursiveLayer(), 0);
			dalTM.startTransaction("");
			validateRecursiveLayer(dalTM.getThreadLocalTransactionInfo().get().getRecursiveLayer(), 1);
			doNothingOutside();
			dalTM.startTransaction("");
			validateRecursiveLayer(dalTM.getThreadLocalTransactionInfo().get().getRecursiveLayer(), 2);
			doNothingInside();
			dalTM.commitTransaction();
			validateRecursiveLayer(dalTM.getThreadLocalTransactionInfo().get().getRecursiveLayer(), 1);
			dalTM.commitTransaction();
			validateRecursiveLayer(dalTM.getThreadLocalTransactionInfo().get().getRecursiveLayer(), 0);

			result = SUCCESS_TAG;
			return result;
		}
		
		private void validateRecursiveLayer(int source, int target) {
			if (source != target) {
				throw new AssertionError("RecursiveLayer check failed."); 
			}
		}
		
	}
	
	public void doNothingInside() {
	}
	
	public void doNothingOutside() {
	}
	
}
