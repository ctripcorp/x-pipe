package com.ctrip.xpipe.redis.console.sentinel;

import com.ctrip.xpipe.api.command.Command;

/**
 * @author lishanglin
 * date 2021/8/31
 */
public interface SentinelBalanceTask extends Command<Void> {

    int getShardsWaitBalances();

    int getTargetUsages();

    int getTotalActiveSize();
}
