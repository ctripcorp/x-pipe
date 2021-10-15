package com.ctrip.xpipe.redis.checker.cluster;

import com.ctrip.xpipe.api.foundation.FoundationService;

import javax.annotation.PostConstruct;

public class AllCheckerLeaderElector extends AbstractCheckerLeaderElector {
    
    private String currentDcId;
    
    public AllCheckerLeaderElector(String currentDcId) {
        this.currentDcId = currentDcId;
    }
    
    @PostConstruct
    public void postConstruct() throws Exception {
        setLeaderAwareClass(AllCheckerLeaderAware.class);
        doStart();
    }
    
    @Override
    protected String getLeaderElectPath() {
        return "/checker/dcleader_" + currentDcId;
    }
}
