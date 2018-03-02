package com.ctrip.xpipe.redis.console.query;

import com.ctrip.xpipe.redis.console.exception.DalUpdateException;
import org.junit.Test;
import org.unidal.dal.jdbc.DalException;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Mar 02, 2018
 */
public class DalQueryHandlerTest {

    DalQueryHandler handler = new DalQueryHandler();

    @Test(expected = DalUpdateException.class)
    public void handleUpdate() throws Exception {
        handler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return 0;
            }
        });
    }

    @Test(expected = DalUpdateException.class)
    public void handleUpdate2() throws Exception {
        handler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                throw new DalException("Resource Not Found");
            }
        });
    }

    @Test(expected = DalUpdateException.class)
    public void handleUpdateBatch() throws Exception {
        handler.handleBatchUpdate(new DalQuery<int[]>() {
            @Override
            public int[] doQuery() throws DalException {
                return new int[0];
            }
        });
    }
}