package com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration.mock;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DataObject;
import org.unidal.dal.jdbc.engine.QueryContext;
import org.unidal.dal.jdbc.query.WriteHandler;

import java.util.function.IntSupplier;

/**
 * @author lishanglin
 * date 2021/4/20
 */
public class MockMysqlWriteHandler implements WriteHandler {

    private IntSupplier delaySupplier;

    private WriteHandler delegate;

    public MockMysqlWriteHandler(WriteHandler mysqlWriteHandler, IntSupplier delaySupplier) {
        this.delegate = mysqlWriteHandler;
        this.delaySupplier = delaySupplier;
    }

    @Override
    public int executeUpdate(QueryContext ctx) throws DalException {
        try {
            Thread.sleep(delaySupplier.getAsInt());
        } catch (Exception e) {
            // ignore
        }

        return delegate.executeUpdate(ctx);
    }

    @Override
    public <T extends DataObject> int[] executeUpdateBatch(QueryContext ctx, T[] protos) throws DalException {
        try {
            Thread.sleep(delaySupplier.getAsInt());
        } catch (Exception e) {
            // ignore
        }

        return delegate.executeUpdateBatch(ctx, protos);
    }

}
