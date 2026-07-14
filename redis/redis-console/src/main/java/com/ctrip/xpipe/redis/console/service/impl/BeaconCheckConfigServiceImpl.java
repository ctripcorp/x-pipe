package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.service.BeaconCheckConfigService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.utils.DateTimeUtils.DEFAULT_OPERATING_UNTIL;

@Service
public class BeaconCheckConfigServiceImpl implements BeaconCheckConfigService {

    private static final Logger logger = LoggerFactory.getLogger(BeaconCheckConfigServiceImpl.class);

    private final DcClusterShardService dcClusterShardService;

    @Autowired
    public BeaconCheckConfigServiceImpl(DcClusterShardService dcClusterShardService) {
        this.dcClusterShardService = dcClusterShardService;
    }

    @Override
    public void stopBeaconCheck(String clusterName, String dc, List<String> shards, int maintainMinutes) throws Exception {
        Date until = DateTimeUtils.getMinutesLaterThan(new Date(), maintainMinutes);
        List<DcClusterShardTbl> matched = findMatched(dc, clusterName, shards);
        int affected = updateOperatingUntil(matched, until);
        // 不主动刷新缓存，等待自动刷新生效
        logger.info("[stopBeaconCheck][{}][{}] requested {}, matched {}, until {}, affected {}",
                clusterName, dc, shards, formatMatchedShards(matched), until, affected);
    }

    @Override
    public void startBeaconCheck(String clusterName, String dc, List<String> shards) throws Exception {
        List<DcClusterShardTbl> matched = findMatched(dc, clusterName, shards);
        int affected = updateOperatingUntil(matched, DEFAULT_OPERATING_UNTIL);
        // 不主动刷新缓存，等待自动刷新生效
        logger.info("[startBeaconCheck][{}][{}] requested {}, matched {}, affected {}",
                clusterName, dc, shards, formatMatchedShards(matched), affected);
    }

    private List<DcClusterShardTbl> findMatched(String dc, String clusterName, List<String> shards) {
        List<DcClusterShardTbl> matched = dcClusterShardService.findDcClusterShardsByNames(dc, clusterName, shards);
        return matched != null ? matched : Collections.emptyList();
    }

    private int updateOperatingUntil(List<DcClusterShardTbl> matched, Date operatingUntil) throws DalException {
        if (matched.isEmpty()) {
            return 0;
        }
        List<Long> dcClusterShardIds = matched.stream()
                .map(DcClusterShardTbl::getDcClusterShardId)
                .collect(Collectors.toList());
        return dcClusterShardService.updateOperatingUntilByIds(dcClusterShardIds, operatingUntil);
    }

    private static String formatMatchedShards(List<DcClusterShardTbl> matched) {
        if (matched.isEmpty()) {
            return "[]";
        }
        return matched.stream()
                .map(shard -> shard.getDcClusterShardId() + ":" + shard.getShardName())
                .collect(Collectors.joining(", ", "[", "]"));
    }
}
