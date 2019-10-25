package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Profile(AbstractProfile.PROFILE_NAME_TEST)
public class TestClusterMetaModifiedNotifier implements ClusterMetaModifiedNotifier {
    @Override
    public void notifyClusterUpdate(String dcName, String clusterName) {

    }

    @Override
    public void notifyClusterDelete(String clusterName, List<DcTbl> dcs) {

    }
}
