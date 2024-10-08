package com.ctrip.xpipe.redis.checker.cluster;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.utils.StringUtil;

import javax.annotation.PostConstruct;

public class AllCheckerLeaderElector extends AbstractCheckerLeaderElector implements TopElement {
    
    private String currentDcId;
    
    public AllCheckerLeaderElector(String currentDcId) {
        setLeaderAwareClass(AllCheckerLeaderAware.class);
        this.currentDcId = currentDcId;
    }
    
    @Override
    protected String getLeaderElectPath() {
        return "/checker/dcleader_" + currentDcId;
    }

    public String getGroupLeaderHostPort() {
        String leaderId = getLeaderId();
        if(StringUtil.isEmpty(leaderId)) {
            return null;
        }
        String port = System.getProperty("server.port", "8080");
        if(leaderId.contains("_")) {
            return leaderId.split("_")[0] + ":" + port;
        } else {
            return leaderId + ":" + port;
        }
    }
}
