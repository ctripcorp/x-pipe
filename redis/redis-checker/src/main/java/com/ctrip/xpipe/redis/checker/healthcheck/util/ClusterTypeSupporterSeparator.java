package com.ctrip.xpipe.redis.checker.healthcheck.util;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClusterTypeSupporterSeparator {

    private ClusterTypeSupporterSeparator() {}

    public static  <T> Map<ClusterType, List<T>> divideByClusterType(List<T> allSupporter) {
        Map<ClusterType, List<T>> clusterTypeToSupporter = Maps.newHashMap();
        clusterTypeToSupporter.put(ClusterType.BI_DIRECTION, allSupporter.stream().filter(supporter -> supporter instanceof BiDirectionSupport).collect(Collectors.toList()));
        clusterTypeToSupporter.put(ClusterType.ONE_WAY, allSupporter.stream().filter(supporter -> supporter instanceof OneWaySupport).collect(Collectors.toList()));
        clusterTypeToSupporter.put(ClusterType.SINGLE_DC, allSupporter.stream().filter(supporter -> supporter instanceof SingleDcSupport).collect(Collectors.toList()));
        clusterTypeToSupporter.put(ClusterType.LOCAL_DC, allSupporter.stream().filter(supporter -> supporter instanceof LocalDcSupport).collect(Collectors.toList()));
        clusterTypeToSupporter.put(ClusterType.CROSS_DC, allSupporter.stream().filter(supporter -> supporter instanceof CrossDcSupport).collect(Collectors.toList()));
        clusterTypeToSupporter.put(ClusterType.HETERO, allSupporter.stream().filter(supporter -> supporter instanceof OneWaySupport).collect(Collectors.toList()));
        return clusterTypeToSupporter;
    }

}
