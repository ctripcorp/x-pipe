package com.ctrip.xpipe.redis.core.config;

import com.ctrip.xpipe.config.ConfigKeyListener;
import com.ctrip.xpipe.config.ZkConfig;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 12:07:43 PM
 */
public interface CoreConfig extends ZkConfig{

    void addListener(ConfigKeyListener listener);

}
