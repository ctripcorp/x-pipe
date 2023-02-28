package com.ctrip.xpipe.redis.checker;

import java.util.List;

public interface DcRelationsService {

    List<String> getTargetDcsByPriority(String clusterName, String downDc, List<String> availableDcs);

    int getDcsDelay(String clusterName, String fromDc, String toDc);

}
