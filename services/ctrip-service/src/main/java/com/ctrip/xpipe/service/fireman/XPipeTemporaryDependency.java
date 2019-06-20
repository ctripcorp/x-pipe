package com.ctrip.xpipe.service.fireman;

import com.ctrip.framework.fireman.container.database.DbNode;
import com.ctrip.framework.fireman.container.manager.MhaManagerNode;
import com.ctrip.framework.fireman.spi.TemporaryDependency;

public class XPipeTemporaryDependency implements TemporaryDependency {

    @Override
    public MhaManagerNode getMhaManagerNode() {
        return XPipeFiremanDependency.Environment.getInstance().getMhaManagerNode();
    }

    @Override
    public DbNode getMaster() {
        return XPipeFiremanDependency.Environment.getInstance().getMasterNode();
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
