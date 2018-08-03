package com.ctrip.xpipe.redis.integratedtest.health;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Aug 06, 2018
 */
public interface XPipeMetaManagerCollector {

    XpipeMetaManager getXPipeMetaManager();

}
