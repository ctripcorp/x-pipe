package com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration.mock;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DataObject;
import org.unidal.dal.jdbc.engine.QueryContext;
import org.unidal.dal.jdbc.query.ReadHandler;

import java.util.List;
import java.util.function.IntSupplier;

/**
 * @author lishanglin
 * date 2021/4/20
 */
public class MockMysqlReadHandler implements ReadHandler {

    private IntSupplier delaySupplier;

    private ReadHandler delegate;

    public MockMysqlReadHandler(ReadHandler mysqlWriteHandler, IntSupplier delaySupplier) {
        this.delegate = mysqlWriteHandler;
        this.delaySupplier = delaySupplier;
    }

    @Override
    public <T extends DataObject> List<T> executeQuery(QueryContext ctx) throws DalException {
        try {
            Thread.sleep(delaySupplier.getAsInt());
        } catch (Exception e) {
            // ignore
        }

        return delegate.executeQuery(ctx);
    }

}
