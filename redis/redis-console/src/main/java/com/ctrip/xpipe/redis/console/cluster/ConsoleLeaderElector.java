package com.ctrip.xpipe.redis.console.cluster;

import com.ctrip.xpipe.cluster.AbstractLeaderElector;
import com.ctrip.xpipe.api.cluster.ClusterServer;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 12, 2017
 */
@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class ConsoleLeaderElector extends AbstractLeaderElector implements ClusterServer{

    public static String KEY_CONSOLE_ID = "CONSOLE_ID";

    @PostConstruct
    public void postContruct() throws Exception {
        doStart();
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
