package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.dao.RouteDao;
import com.ctrip.xpipe.redis.console.model.DcIdNameMapper;
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
        DcIdNameMapper mapper = new DcIdNameMapper.DefaultMapper(dcService);
        List<RouteModel> clone = Lists.transform(routeDao.getAllRoutes(), new Function<RouteTbl, RouteModel>() {
            @Override
            public RouteModel apply(RouteTbl input) {
                return RouteModel.fromRouteTbl(input, mapper);
            }
        });
        return Lists.newArrayList(clone);
    }

    @Override
    public List<RouteModel> getActiveRoutes() {
        DcIdNameMapper mapper = new DcIdNameMapper.DefaultMapper(dcService);
        List<RouteModel> clone = Lists.transform(routeDao.getAllAvailableRoutes(), new Function<RouteTbl, RouteModel>() {
            @Override
            public RouteModel apply(RouteTbl input) {
                return RouteModel.fromRouteTbl(input, mapper);
            }
        });
        return Lists.newArrayList(clone);
    }

    @Override
    public void updateRoute(RouteModel model) {
        DcIdNameMapper mapper = new DcIdNameMapper.DefaultMapper(dcService);
        routeDao.update(model.toRouteTbl(mapper));
    }

    @Override
    public void deleteRoute(long id) {
        routeDao.delete(id);
    }

    @Override
    public void addRoute(RouteModel model) {
        DcIdNameMapper mapper = new DcIdNameMapper.DefaultMapper(dcService);
        routeDao.insert(model.toRouteTbl(mapper));
    }

    @Override
    public boolean existsRouteBetweenDc(String activeDc, String backupDc) {
        List<RouteModel> routes = getActiveRoutes();
        for(RouteModel route : routes) {
            if(route.getSrcDcName().equalsIgnoreCase(backupDc)
                    && route.getDstDcName().equalsIgnoreCase(activeDc))
                return true;
        }
        return false;
    }
}
