package com.ctrip.xpipe.redis.checker.cluster;

import com.ctrip.xpipe.api.cluster.ClusterServer;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.AbstractLeaderElector;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author lishanglin
 * date 2021/3/8
 */
public class CheckerLeaderElector extends AbstractLeaderElector implements ClusterServer {

    public static String KEY_CHECKER_ID = "CHECKER_ID";

    @PostConstruct
    public void postContruct() throws Exception {
        doStart();
    }


    @Override
    protected String getServerId() {

        String id = IpUtils.getFistNonLocalIpv4ServerAddress().getHostAddress();
        String consoleId = System.getProperty(KEY_CHECKER_ID);
        if(!StringUtil.isEmpty(consoleId)){
            id += "_" + consoleId;
        }
        return id;
    }

    @Override
    protected String getLeaderElectPath() {
        String groupId = FoundationService.DEFAULT.getGroupId();
        return "/leader_" + groupId;
    }


    @PreDestroy
    public void preDestroy() throws Exception {
        doStop();
    }
}
