package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.pool.BorrowObjectException;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Sep 10, 2018
 */
public class KeyedObjectPoolTest extends AbstractRedisTest {

    private KeyedObjectPoolForTest pool;

    private ExecutorService executors = DefaultExecutorFactory.createAllowCoreTimeout("test-").createExecutorService();

    @Before
    public void beforeKeyedObjectPoolTest() throws Exception {
        pool = new KeyedObjectPoolForTest();
        LifecycleHelper.initializeIfPossible(pool);
        LifecycleHelper.startIfPossible(pool);
    }

    @Test
    public void testConcurrentTest() throws BorrowObjectException, InterruptedException {
        int N = 5000;
        for (int i = 0; i < N; i++) {
            String key = randomString();
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    for(int j = 0; j < 10; j++) {
                        executors.execute(new AbstractExceptionLogTask() {
                            @Override
                            protected void doRun() throws Exception {
                                SimpleObjectPool<Object> simpleObjectPool = pool.getKeyPool(key);
                                Object o = simpleObjectPool.borrowObject();
                                simpleObjectPool.returnObject(o);
                            }
                        });

                    }
                }
            });

        }
        Thread.sleep(1000 * 60);
    }
}
