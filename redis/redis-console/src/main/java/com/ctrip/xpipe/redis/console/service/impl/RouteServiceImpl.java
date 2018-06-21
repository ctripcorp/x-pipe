package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.dao.RouteDao;
import com.ctrip.xpipe.redis.console.model.RouteTbl;
import com.ctrip.xpipe.redis.console.service.RouteService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */
@Service
public class RouteServiceImpl implements RouteService {

    @Autowired
    private RouteDao routeDao;

    @Override
    public List<RouteTbl> getAllRoutes() {
        return routeDao.getAllAvailableRoutes();
    }
}
