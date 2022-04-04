package com.ctrip.xpipe.redis.core.protocal.pojo;


public enum SentinelFlag {
    s_down, o_down, master, slave, sentinel, disconnected,
    master_down, failover_in_progress, promoted, reconf_sent,
    reconf_inprog, reconf_done
}
