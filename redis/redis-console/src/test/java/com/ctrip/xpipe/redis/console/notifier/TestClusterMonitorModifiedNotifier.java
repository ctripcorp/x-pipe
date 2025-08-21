package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2021/1/18
 */
@Component
@Profile(AbstractProfile.PROFILE_NAME_TEST)
public class TestClusterMonitorModifiedNotifier implements ClusterMonitorModifiedNotifier {

    @Override
    public void notifyClusterUpdate(final String clusterName, long orgId) {

    }

    @Override
    public void notifyClusterDelete(final String clusterName, long orgId) {

    }

}
