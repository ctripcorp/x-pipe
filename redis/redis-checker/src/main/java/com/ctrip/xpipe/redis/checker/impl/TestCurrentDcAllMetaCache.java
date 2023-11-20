package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.meta.CurrentDcAllMeta;

/**
 * @author yu
 * <p>
 * 2023/11/14
 */
public class TestCurrentDcAllMetaCache implements CurrentDcAllMeta {
    @Override
    public DcMeta getCurrentDcAllMeta() {
        return null;
    }
}
