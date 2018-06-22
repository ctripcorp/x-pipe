package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.RouteTbl;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */
public interface RouteService {

    List<RouteTbl> getAllRoutes();
}
