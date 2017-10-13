package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.api.cluster.ClusterServer;
import com.ctrip.xpipe.api.cluster.LeaderAware;
import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 12, 2017
 */
public abstract class AbstractLeaderElector extends AbstractLifecycle implements ApplicationContextAware, ClusterServer {

    @Autowired
    private ZkClient zkClient;

    private ExecutorService executors = Executors.newCachedThreadPool(XpipeThreadFactory.create(getClass().getSimpleName()));

    private ApplicationContext applicationContext;

    private LeaderLatch leaderLatch;

    private volatile boolean isLeader = false;

    @Override
    protected void doStart() throws Exception {

        leaderLatch = new LeaderLatch(zkClient.get(), getLeaderElectPath(), getServerId());
        leaderLatch.addListener(new LeaderLatchListener() {

            @Override
            public void isLeader() {

                logger.info("[isLeader]({})", getServerId());
                isLeader = true;
                Map<String, LeaderAware> leaderawares = applicationContext.getBeansOfType(LeaderAware.class);
                for (Map.Entry<String, LeaderAware> entry : leaderawares.entrySet()) {
                    try{
                        logger.info("[isLeader][notify]{}", entry.getKey());
                        entry.getValue().isleader();
                    }catch (Exception e){
                        logger.error("[isLeader]" + entry, e);
                    }
                }
            }

            @Override
            public void notLeader() {

                logger.info("[notLeader]{}", getServerId());
                isLeader = false;
                Map<String, LeaderAware> leaderawares = applicationContext.getBeansOfType(LeaderAware.class);
                for (Map.Entry<String, LeaderAware> entry : leaderawares.entrySet()) {
                    try{
                        logger.info("[notLeader][notify]{}", entry.getKey());
                        entry.getValue().notLeader();
                    }catch (Exception e){
                        logger.error("[notLeader]" + entry, e);
                    }
                }
            }
        }, executors);
        leaderLatch.start();
    }

    private LockInternalsSorter sorter = new LockInternalsSorter() {
        @Override
        public String fixForSorting(String str, String lockName) {
            return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
        }
    };

    @Override
    public List<String> getAllServers() {

        String leaderElectPath = getLeaderElectPath();
        CuratorFramework curatorFramework = zkClient.get();
        List<String> children = null;
        List<String> result = new LinkedList<>();
        try {
            children = curatorFramework.getChildren().forPath(leaderElectPath);
            children = LockInternals.getSortedChildren("latch-", sorter, children);

            for (String child : children) {
                String currentPath = leaderElectPath + "/" + child;
                result.add(new String(curatorFramework.getData().forPath(currentPath)));
            }
        } catch (Exception e) {
            logger.error("[getAllServers]", e);
        }
        return result;
    }

    protected abstract String getServerId();

    @Override
    protected void doStop() throws Exception {
        leaderLatch.close();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public boolean amILeader() {
        return isLeader;
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    protected abstract String getLeaderElectPath();
}
