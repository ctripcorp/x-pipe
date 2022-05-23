package com.ctrip.xpipe.redis.meta.server.keeper.applier.manager;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.meta.server.exception.MetaServerException;

/**
 * @author ayq
 * <p>
 * 2022/4/6 16:00
 */
public class ApplierStateNotAsExpectedException extends MetaServerException {

    private static final long serialVersionUID = 1L;

    private SlaveRole keeperRole;

    public ApplierStateNotAsExpectedException(ApplierMeta applierMeta, SlaveRole role, Server.SERVER_ROLE expected){
        super(String.format("applier:%s:%d, current:%s, expected:%s", applierMeta.getIp(), applierMeta.getPort(), role.getServerRole(), expected));
        this.keeperRole = role;
        setOnlyLogMessage(true);
    }
}
