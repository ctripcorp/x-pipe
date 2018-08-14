package com.ctrip.xpipe.redis.console.query;

import com.ctrip.xpipe.redis.console.exception.DalInsertException;
import com.ctrip.xpipe.redis.console.exception.DalUpdateException;
import org.junit.Test;
import org.unidal.dal.jdbc.DalException;

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

    @Test
    public void handleDeleteWithFailAsSuccess() throws Exception {
        handler.handleBatchDelete(new DalQuery<int[]>() {
            @Override
            public int[] doQuery() throws DalException {
                return new int[0];
            }
        }, true);
    }

    @Test(expected = DalUpdateException.class)
    public void handleDeleteWithoutFailAsSuccess() throws Exception {
        handler.handleBatchDelete(new DalQuery<int[]>() {
            @Override
            public int[] doQuery() throws DalException {
                return new int[0];
            }
        }, false);
    }

    @Test(expected = DalInsertException.class)
    public void handleInsert() throws Exception {
        handler.handleBatchInsert(new DalQuery<int[]>() {
            @Override
            public int[] doQuery() throws DalException {
                return new int[0];
            }
        });
    }

    @Test
    public void handleInsertPositive() throws Exception {
        handler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return 1;
            }
        });
    }

    @Test(expected = DalUpdateException.class)
    public void testHandleDeleteWithException() throws Exception {
        handler.handleDelete(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                throw new DalException("Dal connect exception");
            }
        }, true);
    }
}