package com.ctrip.xpipe.cluster;

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
public abstract class AbstractLeaderElector extends AbstractLifecycle implements ApplicationContextAware, LeaderLatchListener {

    @Autowired
    private ZkClient zkClient;

    private ExecutorService executors = Executors.newCachedThreadPool(XpipeThreadFactory.create(getClass().getSimpleName()));

    private ApplicationContext applicationContext;

    private LeaderLatch leaderLatch;


    private volatile boolean isLeader = false;

    @Override
    protected void doStart() throws Exception {

        leaderLatch = new LeaderLatch(zkClient.get(), getLeaderElectPath(), getServerId());
        leaderLatch.addListener(this, executors);
        leaderLatch.start();
    }

    private LockInternalsSorter sorter = new LockInternalsSorter() {
        @Override
        public String fixForSorting(String str, String lockName) {
            return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
        }
    };

    public List<String> getAllServers() throws Exception {

        String leaderElectPath = getLeaderElectPath();
        CuratorFramework curatorFramework = zkClient.get();
        List<String> children = curatorFramework.getChildren().forPath(leaderElectPath);

        children = LockInternals.getSortedChildren("latch-", sorter, children);

        List<String> result = new LinkedList<>();

        for (String child : children) {
            String currentPath = leaderElectPath + "/" + child;
            result.add(new String(curatorFramework.getData().forPath(currentPath)));
        }
        return result;
    }


    protected abstract String getServerId();


    @Override
    protected void doStop() throws Exception {
        leaderLatch.close();
    }

    @Override
    public void isLeader() {

        logger.info("[isLeader]({})", getServerId());
        isLeader = true;
        Map<String, LeaderAware> leaderawares = applicationContext.getBeansOfType(LeaderAware.class);
        for (Map.Entry<String, LeaderAware> entry : leaderawares.entrySet()) {
            logger.info("[isLeader][notify]{}", entry.getKey());
            entry.getValue().isleader();
        }
    }

    @Override
    public void notLeader() {

        logger.info("[notLeader]{}", getServerId());
        isLeader = false;
        Map<String, LeaderAware> leaderawares = applicationContext.getBeansOfType(LeaderAware.class);
        for (Map.Entry<String, LeaderAware> entry : leaderawares.entrySet()) {
            logger.info("[notLeader][notify]{}", entry.getKey());
            entry.getValue().notLeader();
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public boolean amILeader() {
        return isLeader;
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    protected abstract String getLeaderElectPath();
}
