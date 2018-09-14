package com.ctrip.xpipe.redis.console.healthcheck.meta;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Sep 13, 2018
 */
public class DefaultMetaChangeManagerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private DefaultMetaChangeManager metaChangeManager;

//    @Before
//    public void beforeDefaultMetaChangeManagerTest() {
//        MockitoAnnotations.initMocks(this);
//    }

    @Test
    public void testStart() {
        metaChangeManager.start();
    }

    @Test
    public void testStop() {
        metaChangeManager.stop();
    }

    @Test
    public void testGetOrCreate() {
    }

    @Test
    public void testIgnore() {
        metaChangeManager.ignore("NULL");
    }

    @Test
    public void testStartIfPossible() {
        metaChangeManager.startIfPossible("NULL");
    }
}