package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.ProxyTbl;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */
public interface ProxyService {

    List<ProxyTbl> getAllProxies();
}
