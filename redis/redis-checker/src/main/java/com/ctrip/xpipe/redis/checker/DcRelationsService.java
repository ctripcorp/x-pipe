package com.ctrip.xpipe.redis.checker;

import java.util.List;

public interface DcRelationsService {

    List<String> getTargetDcsByPriority(String clusterName, String downDc, List<String> availableDcs);

    Integer getDcsDelay(String fromDc, String toDc);

    Integer getClusterDcsDelay(String clusterName, String fromDc, String toDc);

}
