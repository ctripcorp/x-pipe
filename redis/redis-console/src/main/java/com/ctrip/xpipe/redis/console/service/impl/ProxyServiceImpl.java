package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.dao.ProxyDao;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.model.ProxyTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */
@Service
public class ProxyServiceImpl implements ProxyService {

    @Autowired
    private ProxyDao proxyDao;

    @Autowired
    private DcService dcService;

    @Override
    public List<ProxyModel> getActiveProxies() {
        List<ProxyTbl> proxyTbls = proxyDao.getActiveProxyTbls();
        List<ProxyModel> proxies = Lists.newArrayListWithCapacity(proxyTbls.size());
        for(ProxyTbl proxy : proxyTbls) {
            proxies.add(ProxyModel.fromProxyTbl(proxy, dcService));
        }
        return proxies;
    }

    @Override
    public List<ProxyModel> getAllProxies() {
        List<ProxyModel> clone = Lists.transform(proxyDao.getAllProxyTbls(), new Function<ProxyTbl, ProxyModel>() {
            @Override
            public ProxyModel apply(ProxyTbl input) {
                return ProxyModel.fromProxyTbl(input, dcService);
            }
        });
        return Lists.newArrayList(clone);
    }

    @Override
    public void updateProxy(ProxyModel model) {
        proxyDao.update(model.toProxyTbl(dcService));
    }

    @Override
    public void deleteProxy(long id) {
        proxyDao.delete(id);
    }

    @Override
    public void addProxy(ProxyModel model) {
        proxyDao.insert(model.toProxyTbl(dcService));
    }

    @Override
    public List<ProxyTbl> getActiveProxyTbls() {
        return proxyDao.getActiveProxyTbls();
    }
}
