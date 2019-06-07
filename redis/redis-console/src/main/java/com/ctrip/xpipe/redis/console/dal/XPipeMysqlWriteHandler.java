package com.ctrip.xpipe.redis.console.dal;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DataObject;
import org.unidal.dal.jdbc.QueryType;
import org.unidal.dal.jdbc.datasource.DataSourceException;
import org.unidal.dal.jdbc.engine.QueryContext;
import org.unidal.dal.jdbc.query.WriteHandler;
import org.unidal.dal.jdbc.query.mysql.MysqlBaseHandler;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.ContainerLoader;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@Named(type = WriteHandler.class, value = "mysql")
public class XPipeMysqlWriteHandler extends MysqlBaseHandler implements WriteHandler, Initializable {

    @Inject(value = "xpipe")
    private TransactionManager m_transactionManager;

    @Override
    public void initialize() throws InitializationException {
        ContainerLoader.getDefaultContainer().addComponent(this, WriteHandler.class, "mysql");
    }

    @Override
    public int executeUpdate(QueryContext ctx) throws DalException {
        Transaction t = Cat.newTransaction("SQL", getQueryName(ctx));
        DataObject proto = ctx.getProto();
        PreparedStatement ps = null;

        t.addData(ctx.getSqlStatement());

        try {
            ps = createPreparedStatement(ctx, m_transactionManager.getConnection(ctx));

            // Call beforeSave() to do some custom data manipulation
            proto.beforeSave();

            // Setup IN/OUT parameters
            setupInOutParameters(ctx, ps, proto, false);

            // log to CAT
            logCatEvent(ctx);

            // Execute the query
            int rowCount = ps.executeUpdate();

            // Get OUT parameters if have
            retrieveOutParameters(ps, ctx.getParameters(), proto);

            // Retrieve Generated Keys if have
            if (ctx.getQuery().getType() == QueryType.INSERT) {
                retrieveGeneratedKeys(ctx, ps.getGeneratedKeys(), proto);
            }

            t.setStatus(Transaction.SUCCESS);
            return rowCount;
        } catch (DataSourceException e) {
            t.setStatus(e.getClass().getSimpleName());
            Cat.logError(e);

            throw e;
        } catch (Throwable e) {
            t.setStatus(e.getClass().getSimpleName());
            Cat.logError(e);

            throw new DalException(String.format("Error when executing update(%s) failed, proto: %s, message: %s.",
                    ctx.getSqlStatement(), proto, e), e);
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                Cat.logError(e);
            } finally {
                t.complete();
                m_transactionManager.closeConnection();
            }
        }
    }

    @Override
    public <T extends DataObject> int[] executeUpdateBatch(QueryContext ctx, T[] protos) throws DalException {
        Transaction t = Cat.newTransaction("SQL", getQueryName(ctx));
        PreparedStatement ps = null;
        int[] rowCounts = new int[protos.length];
        boolean inTransaction = m_transactionManager.isInTransaction();
        boolean updated = false;

        t.addData(ctx.getSqlStatement());

        try {
            ps = createPreparedStatement(ctx, m_transactionManager.getConnection(ctx));

            if (!inTransaction) {
                ps.getConnection().setAutoCommit(false);
            }

            if (ctx.getQuery().isStoreProcedure()) {
                for (int i = 0; i < protos.length; i++) {
                    // Call beforeSave() to do some custom data manipulation
                    protos[i].beforeSave();

                    // Setup IN/OUT parameters
                    setupInOutParameters(ctx, ps, protos[i], false);

                    if (i == 0) {
                        // log to CAT
                        logCatEvent(ctx);
                    }

                    // Execute the query
                    rowCounts[i] = ps.executeUpdate();
                    updated = true;

                    // Get OUT parameters if have
                    retrieveOutParameters(ps, ctx.getParameters(), protos[i]);

                    // Retrieve Generated Keys if have
                    if (ctx.getQuery().getType() == QueryType.INSERT) {
                        retrieveGeneratedKeys(ctx, ps.getGeneratedKeys(), protos[i]);
                    }
                }
            } else {
                for (int i = 0; i < protos.length; i++) {
                    // Call beforeSave() to do some custom data manipulation
                    protos[i].beforeSave();

                    // Setup IN/OUT parameters
                    setupInOutParameters(ctx, ps, protos[i], false);

                    if (i == 0) {
                        // log to CAT
                        logCatEvent(ctx);
                    }

                    ps.addBatch();
                }

                rowCounts = ps.executeBatch();
                updated = true;

                // Retrieve Generated Keys if have
                if (ctx.getQuery().getType() == QueryType.INSERT) {
                    retrieveGeneratedKeys(ctx, ps.getGeneratedKeys(), protos);
                }
            }

            if (!inTransaction && updated) {
                ps.getConnection().commit();
                ps.getConnection().setAutoCommit(true);
            }

            t.setStatus(Transaction.SUCCESS);
            return rowCounts;
        } catch (DataSourceException e) {
            t.setStatus(e.getClass().getSimpleName());
            Cat.logError(e);

            throw e;
        } catch (Throwable e) {
            if (!inTransaction && updated) {
                try {
                    ps.getConnection().rollback();
                    ps.getConnection().setAutoCommit(true);
                } catch (SQLException sqle) {
                    if (e instanceof SQLException) {
                        ((SQLException) e).setNextException(sqle);
                    }
                }
            }

            t.setStatus(e.getClass().getSimpleName());
            Cat.logError(e);

            throw new DalException(String.format("Error when executing batch update(%s) failed, proto: %s, message: %s.",
                    ctx.getSqlStatement(), ctx.getProto(), e), e);
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                Cat.logError(e);
            } finally {
                t.complete();
                m_transactionManager.closeConnection();
            }
        }
    }
}