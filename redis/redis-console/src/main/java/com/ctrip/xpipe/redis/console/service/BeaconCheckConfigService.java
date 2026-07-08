package com.ctrip.xpipe.redis.console.service;

import org.unidal.dal.jdbc.DalException;

import java.util.List;

public interface BeaconCheckConfigService {

    void stopBeaconCheck(String clusterName, String dc, List<String> shards, int maintainMinutes) throws Exception;

    void startBeaconCheck(String clusterName, String dc, List<String> shards) throws Exception;
}
