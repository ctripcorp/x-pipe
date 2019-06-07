package com.ctrip.xpipe.redis.console.dal;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DataObject;
import org.unidal.dal.jdbc.datasource.DataSourceException;
import org.unidal.dal.jdbc.engine.QueryContext;
import org.unidal.dal.jdbc.entity.DataObjectAssembly;
import org.unidal.dal.jdbc.query.ReadHandler;
import org.unidal.dal.jdbc.query.WriteHandler;
import org.unidal.dal.jdbc.query.mysql.MysqlBaseHandler;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.ContainerLoader;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Named(type = ReadHandler.class, value = "mysql")
public class XPipeMysqlReadHandler extends MysqlBaseHandler implements ReadHandler, Initializable {

    @Inject(value = "xpipe")
    private TransactionManager m_transactionManager;

    @Inject
    private DataObjectAssembly m_assembly;

    @Override
    public void initialize() throws InitializationException {
        // Override/Replace the default read handler
        ContainerLoader.getDefaultContainer().addComponent(this, ReadHandler.class, "mysql");
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends DataObject> List<T> executeQuery(QueryContext ctx) throws DalException {
        Transaction t = Cat.newTransaction("SQL", getQueryName(ctx));
        T proto = (T) ctx.getProto();
        PreparedStatement ps = null;

        t.addData(ctx.getSqlStatement());

        try {
            ps = createPreparedStatement(ctx, m_transactionManager.getConnection(ctx));

            // Set fetch size if have
            if (ctx.getFetchSize() > 0) {
                ps.setFetchSize(ctx.getFetchSize());
            }

            // Setup IN/OUT parameters
            setupInOutParameters(ctx, ps, proto, true);

            // log to CAT
            logCatEvent(ctx);

            // Execute the query
            ResultSet rs = ps.executeQuery();

            // Fetch all rows
            List<T> rows = m_assembly.assemble(ctx, rs);

            // Get OUT parameters if have
            retrieveOutParameters(ps, ctx.getParameters(), proto);

            t.setStatus(Transaction.SUCCESS);
            return rows;
        } catch (DataSourceException e) {
            t.setStatus(e.getClass().getSimpleName());
            Cat.logError(e);

            throw e;
        } catch (Throwable e) {
            t.setStatus(e.getClass().getSimpleName());
            Cat.logError(e);

            throw new DalException(String.format("Error when executing query(%s) failed, proto: %s, message: %s.",
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

}
