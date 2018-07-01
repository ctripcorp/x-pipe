package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.dao.ProxyDao;
import com.ctrip.xpipe.redis.console.model.ProxyTbl;
import com.ctrip.xpipe.redis.console.model.ProxyTblDao;
import com.ctrip.xpipe.redis.console.service.ProxyService;
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

    @Override
    public List<ProxyTbl> getAllProxies() {
        return proxyDao.getAllProxyTbls();
    }
}
