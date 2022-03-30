package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.RouteModel;
import com.ctrip.xpipe.redis.console.model.RouteTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteInfoModel;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */
public interface RouteService {

    List<RouteTbl> getActiveRouteTbls();

    List<RouteModel> getAllRoutes();

    List<RouteModel> getActiveRoutes();

    void updateRoute(RouteModel model);

    void updateRoute(RouteInfoModel model);

    void deleteRoute(long id);

    void addRoute(RouteModel model);

    void addRoute(RouteInfoModel model);

    boolean existsRouteBetweenDc(String activeDc, String backupDc);

    boolean existPeerRoutes(String currentDc, String clusterId);

    List<RouteInfoModel> getAllActiveRouteInfos();

    List<RouteInfoModel> getAllActiveRouteInfosByTag(String tag);

    RouteInfoModel getRouteInfoById(long routeId);
}
