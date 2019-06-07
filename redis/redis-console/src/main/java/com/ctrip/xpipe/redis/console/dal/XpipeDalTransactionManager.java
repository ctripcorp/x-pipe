package com.ctrip.xpipe.redis.console.dal;

import com.dianping.cat.Cat;
import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.dal.jdbc.DalRuntimeException;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.dal.jdbc.engine.QueryContext;
import org.unidal.dal.jdbc.mapping.TableProvider;
import org.unidal.dal.jdbc.mapping.TableProviderManager;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author shyin
 *
 * Aug 26, 2016
 */
@Named(type = TransactionManager.class, value="xpipe")
public class XpipeDalTransactionManager extends ContainerHolder implements TransactionManager, LogEnabled, Initializable {
	public static int INITIAL_STATUS = 0;
	public static int PARENT_TRANSACTION = 1;

	private static ThreadLocalTransactionInfo m_threadLocalData = new ThreadLocalTransactionInfo();

	@Inject
	private TableProviderManager m_tableProviderManager;

	@Inject
	private DataSourceManager m_dataSourceManager;

	private Logger m_logger;

	protected ThreadLocalTransactionInfo getThreadLocalTransactionInfo() {
		   return m_threadLocalData;
	}

	@Override
	public void initialize() {
		// Override the default transaction manager
		getContainer().addComponent(this, TransactionManager.class, "default");
	}

	@Override
	public void closeConnection() {
	  TransactionInfo trxInfo = m_threadLocalData.get();

	  if (!trxInfo.isInTransaction()) {
		 try {
			trxInfo.reset();
		 } catch (SQLException e) {
			m_logger.warn("Error when closing Connection, message: " + e, e);
		 }
	  }
	}

	private void closeConnection(Connection connection) {
	  if (connection != null) {
		 try {
			connection.close();
		 } catch (SQLException e) {
			// ignore it
			 m_logger.warn("Error when closing Connection, message: " + e, e);
		 }
	  }
	}

	@Override
	public void commitTransaction() {
	  TransactionInfo trxInfo = m_threadLocalData.get();

	  if (!trxInfo.isInTransaction()) {
		 throw new DalRuntimeException("There is no active transaction open, can't commit");
	  }

	  if(trxInfo.getRecursiveLayer() <= INITIAL_STATUS) {
		  throw new DalRuntimeException("Invalid transaction commit");
	  }

	  if( PARENT_TRANSACTION == trxInfo.getRecursiveLayer()) {
		  try {
			 if (trxInfo.getConnection() != null) {
				trxInfo.getConnection().commit();
			 }
		  } catch (SQLException e) {
			 throw new DalRuntimeException("Unable to commit transaction, message: " + e, e);
		  } finally {
			 try {
				trxInfo.reset();
			 } catch (SQLException e) {
				 m_logger.warn("Error when commitTransaction, message: " + e, e);
			 }
		  }
	  } else {
		  trxInfo.decrRecursiveLayer();
	  }

	}

	public void enableLogging(Logger logger) {
	  m_logger = logger;
	}

	@Override
	public Connection getConnection(QueryContext ctx) {
	  String logicalName = ctx.getEntityInfo().getLogicalName();
	  TableProvider tableProvider = m_tableProviderManager.getTableProvider(logicalName);
	  String dataSourceName = tableProvider.getDataSourceName(ctx.getQueryHints(), logicalName);
	  TransactionInfo trxInfo = m_threadLocalData.get();

	  ctx.setDataSourceName(dataSourceName);

	  if (trxInfo.isInTransaction()) {
		 if (dataSourceName.equals(trxInfo.getDataSourceName())) {
			return trxInfo.getConnection();
		 } else {
			throw new DalRuntimeException("Only one datasource can participate in a transaction. Now: "
				  + trxInfo.getDataSourceName() + ", you provided: " + dataSourceName);
		 }
	  } else { // Not in transaction
		 DataSource dataSource = m_dataSourceManager.getDataSource(dataSourceName);
		 Connection connection = null;
		 SQLException exception = null;

		 try {
			connection = trxInfo.getConnection();

			if (connection == null) {
			   connection = dataSource.getConnection();
			}

			connection.setAutoCommit(true);
		 } catch (SQLException e) {
			exception = e;
		 }

		 // retry once if pooled connection is closed by server side
		 if (exception != null) {
			closeConnection(connection);

			m_logger.warn(String.format("Iffy database(%s) connection closed, try to reconnect.", dataSourceName),
				  exception);

			try {
			   connection = dataSource.getConnection();
			   connection.setAutoCommit(true);
			   exception = null;
			} catch (SQLException e) {
			   closeConnection(connection);
			   m_logger.warn(String.format("Unable to reconnect to database(%s).", dataSourceName), e);
			}
		 }

		 if (exception != null) {
			throw new DalRuntimeException("Error when getting connection from DataSource(" + dataSourceName
				  + "), message: " + exception, exception);
		 } else {
			trxInfo.setConnection(connection);
			trxInfo.setDataSourceName(dataSourceName);
			trxInfo.setInTransaction(false);
			return connection;
		 }
	  }
	}

	@Override
	public boolean isInTransaction() {
	  TransactionInfo trxInfo = m_threadLocalData.get();

	  return trxInfo.isInTransaction();
	}

	@Override
	public void rollbackTransaction() {
	  TransactionInfo trxInfo = m_threadLocalData.get();

	  if (!trxInfo.isInTransaction()) {
		 throw new DalRuntimeException("There is no active transaction open, can't tryRollback");
	  }
	  if(trxInfo.getRecursiveLayer() <= INITIAL_STATUS) {
		  throw new DalRuntimeException("Invalid transaction tryRollback");
	  }

	  if(PARENT_TRANSACTION == trxInfo.getRecursiveLayer()) {
		  try {
			 if (trxInfo.getConnection() != null) {
				trxInfo.getConnection().rollback();
			 }
		  } catch (SQLException e) {
			 throw new DalRuntimeException("Unable to tryRollback transaction, message: " + e, e);
		  } finally {
			 try {
				trxInfo.reset();
			 } catch (SQLException e) {
				Cat.logError(e);
			 }
		  }
	  } else {
		  trxInfo.decrRecursiveLayer();
	  }

	}

	@Override
	public void startTransaction(String datasource) {
	  TransactionInfo trxInfo = m_threadLocalData.get();

	  if(trxInfo.getRecursiveLayer() < INITIAL_STATUS ) {
		  throw new DalRuntimeException("Cannot start transaction.");
	  }

	  if(INITIAL_STATUS == trxInfo.getRecursiveLayer()) {
		  DataSource ds = m_dataSourceManager.getDataSource(datasource);
		  Connection connection = null;
		  try {
				connection = ds.getConnection();
				connection.setAutoCommit(false);
				trxInfo.setConnection(connection);
				trxInfo.setDataSourceName(datasource);
				trxInfo.setInTransaction(true);
				trxInfo.incrRecursiveLayer();
			  } catch (SQLException e) {
				closeConnection(connection);
				throw new DalRuntimeException("Error when getting connection from DataSource(" + datasource
					  + "), message: " + e, e);
			  }
	  } else {
		  trxInfo.incrRecursiveLayer();
	  }

	}



	static class ThreadLocalTransactionInfo extends ThreadLocal<TransactionInfo> {
	      @Override
	      protected TransactionInfo initialValue() {
	         return new TransactionInfo();
	      }
	   }

	static class TransactionInfo {
	  private String m_dataSourceName;

	  private Connection m_connection;

	  private boolean m_inTransaction;

	  private int m_recursiveLayer;

	  public Connection getConnection() {
		 return m_connection;
	  }

	  public String getDataSourceName() {
		 return m_dataSourceName;
	  }

	  public boolean isInTransaction() {
		 try {
			if (m_connection != null && m_connection.isClosed()) {
			   return false;
			}
		 } catch (SQLException e) {
			Cat.logError(e);
		 }

		 return m_inTransaction;
	  }

	  public int getRecursiveLayer() {
		  return m_recursiveLayer;
	  }

	  public void reset() throws SQLException {
			 if (m_connection != null) {
				m_connection.close();
			 }

			 m_connection = null;
			 m_dataSourceName = null;
			 m_inTransaction = false;
			 m_recursiveLayer = 0;
			 m_threadLocalData.remove();

	  }

	  public void setConnection(Connection connection) {
		 m_connection = connection;
	  }

	  public void setDataSourceName(String dataSourceName) {
		 m_dataSourceName = dataSourceName;
	  }

	  public void setInTransaction(boolean inTransaction) {
		 m_inTransaction = inTransaction;
	  }

	  public void incrRecursiveLayer() {
		  ++m_recursiveLayer;
	  }

	  public void decrRecursiveLayer() {
		  --m_recursiveLayer;
	  }

	}
}
