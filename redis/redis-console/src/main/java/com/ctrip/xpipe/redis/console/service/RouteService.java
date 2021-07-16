package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.RouteModel;
import com.ctrip.xpipe.redis.console.model.RouteTbl;

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

    void deleteRoute(long id);

    void addRoute(RouteModel model);

    boolean existsRouteBetweenDc(String activeDc, String backupDc);

    boolean existPeerRoutes(String currentDc, String clusterId);
}
