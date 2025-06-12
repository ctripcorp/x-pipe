package com.ctrip.xpipe.redis.console.cluster;

import com.ctrip.xpipe.api.cluster.ClusterServer;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.cluster.AbstractLeaderElector;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;

import javax.annotation.PreDestroy;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 12, 2017
 */
public class ConsoleLeaderElector extends AbstractLeaderElector implements ClusterServer, TopElement {

    private AtomicInteger forceHealthy = new AtomicInteger(0);

    public static String KEY_CONSOLE_ID = "CONSOLE_ID";

    public ConsoleLeaderElector() {
        setLeaderAwareClass(ConsoleLeaderAware.class);
    }

    public void forceSetLeader() {
        logger.info("[forceSetLeader]");
        getZKEventExecutors().execute(() -> setForceHealthy(1));
    }

    public void forceSetFollower() {
        logger.info("[forceSetFollower]");
        getZKEventExecutors().execute(() -> setForceHealthy(-1));
    }

    public void forceReset() {
        logger.info("[forceReset]");
        getZKEventExecutors().execute(() -> setForceHealthy(0));
    }

    private void setForceHealthy(int healthy) {
        getZKEventExecutors().execute(() -> {
            logger.debug("[setForceHealthy] zk:{} force:{}->{}",amILeader(), forceHealthy.get(), healthy);
            boolean before = amILeader();
            this.forceHealthy.set(healthy);
            boolean after = amILeader();

            if (!before && after) {
                logger.info("[setForceHealthy][become leader]");
                this.notifyBecomeLeader();
            } else if (before && !after) {
                logger.info("[setForceHealthy][lose leader]");
                this.notifyLoseLeader();
            }
        });
    }

    @Override
    public boolean amILeader() {
        int localForceHealthy = forceHealthy.get();
        if (localForceHealthy > 0) {
            return true;
        } else if (localForceHealthy < 0) {
            return false;
        } else {
            return super.amILeader();
        }
    }

    @Override
    protected String getServerId() {

        String id = IpUtils.getFistNonLocalIpv4ServerAddress().getHostAddress();
        String consoleId = System.getProperty(KEY_CONSOLE_ID);
        if(!StringUtil.isEmpty(consoleId)){
            id += "_" + consoleId;
        }
        return id;
    }

    @Override
    protected String getLeaderElectPath() {
        return "/leader";
    }


    @PreDestroy
    public void preDestroy() throws Exception {
        doStop();
    }
}
