package com.ctrip.xpipe.redis.meta.server.zk;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 29, 2017
 */
public class TestPathChildren extends AbstractMetaServerTest {

    @Test
    public void testPathChildern() throws Exception {

        DefaultZkConfig zkConfig = new DefaultZkConfig();
        zkConfig.setZkConnectionTimeoutMillis(1000);
        String path = "/" + getTestName();

        logger.info("[before create]");
        CuratorFramework curatorFramework = zkConfig.create("10.2.38.87");
        logger.info("[after create]");
        PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework, path, true);
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                logger.info("{}", event);
//                printAllData(pathChildrenCache);
            }
        });

        try {
            logger.info("[begin start]");
            pathChildrenCache.start();
        } catch (Exception e) {
            logger.error("[start]", e);
        }

        createPath(curatorFramework, path, 3);
        logger.info("[simple getChildren]{}", curatorFramework.getChildren().forPath(path));

        waitForAnyKey();
    }

    private void createPath(CuratorFramework curatorFramework, String path, int count) {

        for (int i = 0; i < count; i++) {

            int finalI = i;
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(String.format("%s/%d", path, finalI));
                    } catch (Exception e) {
                        logger.error("[run]", e);
                    }
                }
            });
        }


    }

    private void printAllData(PathChildrenCache pathChildrenCache) {

        for (ChildData childData : pathChildrenCache.getCurrentData()) {
            logger.info("{}", childData);
        }
    }
}
