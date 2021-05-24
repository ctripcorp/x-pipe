package com.ctrip.xpipe.redis.checker.alert;

import java.util.Set;

/**
 * @author lishanglin
 * date 2021/5/19
 */
public interface AlertDbConfig {

    boolean shouldClusterAlert(String cluster);

    Set<String> clusterAlertWhiteList();

}
