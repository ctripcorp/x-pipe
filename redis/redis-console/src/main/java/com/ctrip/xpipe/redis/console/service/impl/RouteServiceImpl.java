package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.dao.RouteDao;
import com.ctrip.xpipe.redis.console.model.RouteModel;
import com.ctrip.xpipe.redis.console.model.RouteTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.RouteService;
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
public class RouteServiceImpl implements RouteService {

    @Autowired
    private RouteDao routeDao;

    @Autowired
    private DcService dcService;

    @Override
    public List<RouteTbl> getActiveRouteTbls() {
        return routeDao.getAllAvailableRoutes();
    }

    @Override
    public List<RouteModel> getAllRoutes() {
        List<RouteModel> clone = Lists.transform(routeDao.getAllRoutes(), new Function<RouteTbl, RouteModel>() {
            @Override
            public RouteModel apply(RouteTbl input) {
                return RouteModel.fromRouteTbl(input, dcService);
            }
        });
        return Lists.newArrayList(clone);
    }

    @Override
    public List<RouteModel> getActiveRoutes() {
        List<RouteModel> clone = Lists.transform(routeDao.getAllAvailableRoutes(), new Function<RouteTbl, RouteModel>() {
            @Override
            public RouteModel apply(RouteTbl input) {
                return RouteModel.fromRouteTbl(input, dcService);
            }
        });
        return Lists.newArrayList(clone);
    }

    @Override
    public void updateRoute(RouteModel model) {
        routeDao.update(model.toRouteTbl(dcService));
    }

    @Override
    public void deleteRoute(long id) {
        routeDao.delete(id);
    }

    @Override
    public void addRoute(RouteModel model) {
        routeDao.insert(model.toRouteTbl(dcService));
    }
}
