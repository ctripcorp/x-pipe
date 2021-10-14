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
public class GroupCheckerLeaderElector extends AbstractCheckerLeaderElector {

    private String groupId;
    
    public GroupCheckerLeaderElector(String groupId) {
        this.groupId = groupId;
    }
    
    @PostConstruct
    public void postContruct() throws Exception {
        setLeaderAwareClass(GroupCheckerLeaderAware.class);
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
        return "/leader_" + groupId;
    }


    @PreDestroy
    public void preDestroy() throws Exception {
        doStop();
    }
}
