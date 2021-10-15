package com.ctrip.xpipe.redis.checker.cluster;

import com.ctrip.xpipe.api.cluster.ClusterServer;
import com.ctrip.xpipe.cluster.AbstractLeaderElector;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;

public abstract class AbstractCheckerLeaderElector extends AbstractLeaderElector implements ClusterServer {
    public static String KEY_CHECKER_ID = "CHECKER_ID";

    @Override
    protected String getServerId() {
        String id = IpUtils.getFistNonLocalIpv4ServerAddress().getHostAddress();
        String consoleId = System.getProperty(KEY_CHECKER_ID);
        if(!StringUtil.isEmpty(consoleId)) {
            id += "_" + consoleId;
        }
        return id;
    }
}
