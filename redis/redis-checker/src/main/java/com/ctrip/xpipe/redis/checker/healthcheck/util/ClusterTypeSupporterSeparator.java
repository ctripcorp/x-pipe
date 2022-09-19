package com.ctrip.xpipe.redis.checker.healthcheck.util;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.tuple.Pair;
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

    public static <T> Map<Pair<ClusterType, DcGroupType>, List<T>> divideByClusterTypeAndGroupType(List<T> allSupporter) {
        Map<Pair<ClusterType, DcGroupType>, List<T>> clusterTypeToSupporter = Maps.newHashMap();
        clusterTypeToSupporter.put(new Pair<>(ClusterType.BI_DIRECTION, DcGroupType.DR_MASTER), allSupporter.stream().filter(supporter -> supporter instanceof BiDirectionSupport).collect(Collectors.toList()));
        clusterTypeToSupporter.put(new Pair<>(ClusterType.ONE_WAY, DcGroupType.DR_MASTER), allSupporter.stream().filter(supporter -> supporter instanceof OneWaySupport).collect(Collectors.toList()));
        clusterTypeToSupporter.put(new Pair<>(ClusterType.SINGLE_DC, DcGroupType.DR_MASTER), allSupporter.stream().filter(supporter -> supporter instanceof SingleDcSupport).collect(Collectors.toList()));
        clusterTypeToSupporter.put(new Pair<>(ClusterType.LOCAL_DC, DcGroupType.DR_MASTER), allSupporter.stream().filter(supporter -> supporter instanceof LocalDcSupport).collect(Collectors.toList()));
        clusterTypeToSupporter.put(new Pair<>(ClusterType.CROSS_DC, DcGroupType.DR_MASTER), allSupporter.stream().filter(supporter -> supporter instanceof CrossDcSupport).collect(Collectors.toList()));
        clusterTypeToSupporter.put(new Pair<>(ClusterType.HETERO, DcGroupType.DR_MASTER), allSupporter.stream().filter(supporter -> supporter instanceof OneWaySupport).collect(Collectors.toList()));
        clusterTypeToSupporter.put(new Pair<>(ClusterType.HETERO, DcGroupType.MASTER), allSupporter.stream().filter(supporter -> supporter instanceof SingleDcSupport).collect(Collectors.toList()));
        return clusterTypeToSupporter;
    }

}
