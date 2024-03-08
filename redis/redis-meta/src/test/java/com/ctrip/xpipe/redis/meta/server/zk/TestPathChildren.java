package com.ctrip.xpipe.redis.meta.server.zk;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 29, 2017
 */
public class TestPathChildren extends AbstractMetaServerTest {

    private int count = 4;

    private CuratorFramework client;

    private String path;

    @Before
    public void beforeTestPathChildren() throws Exception {

        DefaultZkConfig zkConfig = new DefaultZkConfig("10.2.38.87");
        zkConfig.setZkConnectionTimeoutMillis(1000);

        path = "/" + getTestName();
        logger.info("[before create]");
        client = zkConfig.create();
        logger.info("[after create]");

        if (client.checkExists().forPath(path) != null) {
            client.delete().deletingChildrenIfNeeded().forPath(path);
        }
    }

    @Test
    public void testPathChildernAlreadyExist() throws Exception {


        createPath(client, path, 0, count);

        CountDownLatch latch = new CountDownLatch(count);
        List<String> result = new LinkedList<>();

        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, path, true);
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {

            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

                try {
                    sleep(50);
                    logger.info("event");
                    int realSize = pathChildrenCache.getCurrentData().size();
                    if (count != realSize) {
                        String desc = String.format("expected:%d, real:%d", count, realSize);
                        result.add(desc);
                        logger.info("expected:{}, real:{}", count, realSize);
                    }
                } finally {
                    latch.countDown();
                }
            }
        });

        try {
            pathChildrenCache.start();
        } catch (Exception e) {
            logger.error("[start]", e);
        }

        latch.await();
        Assert.assertEquals(0, result.size());


    }

    private void createPath(CuratorFramework curatorFramework, String path, int start, int count) throws Exception {

        curatorFramework.createContainers(path);

        for (int i = 0; i < count; i++) {
            try {
                curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(
                        String.format("%s/%d", path, start + i),
                        randomString(1 << 15).getBytes()
                );
            } catch (Exception e) {
                logger.error("[run]", e);
            }
        }
    }

    private void printAllData(PathChildrenCache pathChildrenCache) {

        logger.info("[begin]-------------");
        for (ChildData childData : pathChildrenCache.getCurrentData()) {
            logger.info("{}", childData.getPath());
        }
        logger.info("[end]-------------");
    }
}
