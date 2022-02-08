package com.ctrip.xpipe.redis.console.migration;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationEvent;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Maps;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Map;
import java.util.concurrent.*;

import static org.mockito.Mockito.*;

public class DoMigrationIntegrationTest extends AbstractMigrationIntegrationTest {

    private Map<Long, MigrationEvent> events = Maps.newConcurrentMap();

    @Before
    public void beforeDoMigrationIntegrationTest() {
        setDelay(false);
        when(migrationEventManager.getEvent(anyLong())).thenAnswer(new Answer<MigrationEvent>() {
            @Override
            public MigrationEvent answer(InvocationOnMock invocationOnMock) throws Throwable {
                long eventId = (Long) invocationOnMock.getArguments()[0];
                if (events.containsKey(eventId)) {
                    return events.get(eventId);
                } else {
                    MigrationEvent event = loadMigrationEvent(migrationEventDetails(eventId));
                    events.put(eventId, event);
                    return event;
                }
            }
        });
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (isDelay()) {
                    Thread.sleep(randomInt(6, 27));
                }
                return null;
            }
        }).when(migrationService).updateMigrationShardLogById(anyLong(), anyString());

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (isDelay()) {
                    Thread.sleep(randomInt(6, 13));
                }
                return null;
            }
        }).when(migrationService).updatePublishInfoById(anyLong(), anyString());

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (isDelay()) {
                    Thread.sleep(randomInt(10, 34));
                }
                return null;
            }
        }).when(clusterService).updateStatusById(anyLong(), any(), anyLong());

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (isDelay()) {
                    Thread.sleep(randomInt(38, 178));
                }
                return null;
            }
        }).when(redisService).updateBatchMaster(any());

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (isDelay()) {
                    Thread.sleep(randomInt(5, 20));
                }
                return null;
            }
        }).when(clusterService).updateActivedcId(anyLong(), anyLong());
    }

    @After
    public void afterDoMigrationIntegrationTest() throws Exception {

    }

    @Test
    public void testContinueMigration() throws Exception {
        migrationService.continueMigrationEvent(1000L);
        long start = System.nanoTime();
        Thread.sleep(1000);
        waitConditionUntilTimeOut(()->migrationService.getMigrationEvent(1000L).isDone(), 30000);
        logger.warn("[duration] {}", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    }

    @Test
    public void test500Clusters() throws Exception {
        int tomcatDefaultThread = 10;
        int tomcatMaxThread = 200;
        ExecutorService tomcatSimulator = new ThreadPoolExecutor(tomcatDefaultThread,
                tomcatMaxThread,
                120L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                XpipeThreadFactory.create("http-nio"),
                new ThreadPoolExecutor.AbortPolicy());
        ((ThreadPoolExecutor) tomcatSimulator).prestartAllCoreThreads();
        long start = System.nanoTime();
        int clusterNum = 500;
        for (int i = 0; i < clusterNum; i++) {
            int eventId = i;
            tomcatSimulator.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        migrationService.continueMigrationEvent(eventId);
                    } catch (Exception e) {
                        logger.info("[test500Clusters] run {} fail", eventId, e);
                    }
                }
            });
        }
        Thread.sleep(25 * 1000);
        waitConditionUntilTimeOut(()->isAllClusterMigrationDone(0, 500), 5 * 60 * 1000);
        logger.warn("[duration] {}", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    }


    private boolean isAllClusterMigrationDone(int startId, int endId) {
        for (int i = startId; i < endId; i++) {
            if (!migrationService.getMigrationEvent(i).isDone()) {
                logger.warn("[not complete] {}, {}", i, migrationService.getMigrationEvent(i).getMigrationClusters());
                return false;
            }
        }
        return true;
    }

}
