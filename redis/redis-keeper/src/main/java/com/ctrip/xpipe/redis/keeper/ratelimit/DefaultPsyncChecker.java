package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;

import java.io.IOException;

/**
 * @author chen.zhu
 * <p>
 * Mar 02, 2020
 */
public class DefaultPsyncChecker implements PsyncChecker {
    @Override
    public boolean canSendPsync() {
        return false;
    }

    @Override
    public void onFullSync() {

    }

    @Override
    public void reFullSync() {

    }

    @Override
    public void beginWriteRdb(EofType eofType, long masterRdbOffset) throws IOException {

    }

    @Override
    public void endWriteRdb() {

    }

    @Override
    public void onContinue(String requestReplId, String responseReplId) {

    }
}
