package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.model.ProxyTbl;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */
public interface ProxyService {

    List<ProxyModel> getActiveProxies();

    List<ProxyModel> getAllProxies();

    void updateProxy(ProxyModel model);

    void deleteProxy(long id);

    void addProxy(ProxyModel model);

    List<ProxyTbl> getActiveProxyTbls();
}
