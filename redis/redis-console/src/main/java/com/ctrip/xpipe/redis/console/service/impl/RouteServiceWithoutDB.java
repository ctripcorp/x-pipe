package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.DcIdNameMapper;
import com.ctrip.xpipe.redis.console.model.RouteModel;
import com.ctrip.xpipe.redis.console.model.RouteTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteDirectionModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteInfoModel;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(true)
public class RouteServiceWithoutDB extends RouteServiceImpl {

    private TimeBoundCache<List<RouteModel>> allRouteCache;

    @Autowired
    private ConsolePortalService consolePortalService;

    @Autowired
    private ConsoleConfig consoleConfig;

    @PostConstruct
    public void init() {
        allRouteCache = new TimeBoundCache<>(consoleConfig::getCacheRefreshInterval, consolePortalService::getActiveRoutes);
    }

    @Override
    public List<RouteTbl> getActiveRouteTbls() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RouteModel> getAllRoutes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RouteModel> getActiveRoutes() {
        return allRouteCache.getData();
    }

    @Override
    public void updateRoute(RouteModel model) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRoute(RouteInfoModel model) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteRoute(long id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRoutes(List<RouteInfoModel> models) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addRoute(RouteModel model) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addRoute(RouteInfoModel model) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean existPeerRoutes(String currentDc, String clusterId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RouteInfoModel> getAllActiveRouteInfoModels() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RouteInfoModel> getAllActiveRouteInfoModelsByTagAndSrcDcName(String tag, String srcDcName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RouteInfoModel> getAllActiveRouteInfoModelsByTag(String tag) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RouteInfoModel> getAllActiveRouteInfoModelsByTagAndDirection(String tag, String srcDcName, String dstDcName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RouteInfoModel getRouteInfoModelById(long routeId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RouteDirectionModel> getAllRouteDirectionModelsByTag(String tag) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RouteInfoModel convertRouteTblToRouteInfoModel(RouteTbl routeTbl, DcIdNameMapper dcIdNameMapper, Map<Long, String> proxyIdUriMap) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, RouteInfoModel> getRouteIdInfoModelMap() {
        throw new UnsupportedOperationException();
    }
}
