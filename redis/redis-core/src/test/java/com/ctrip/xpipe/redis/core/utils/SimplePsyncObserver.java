package com.ctrip.xpipe.redis.core.utils;

import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.google.common.util.concurrent.SettableFuture;

import java.io.IOException;

/**
 * @author wenchao.meng
 * Mar 17, 2021
 */
public class SimplePsyncObserver implements PsyncObserver {

    private SettableFuture<Boolean> online =  SettableFuture.create();

    @Override
    public void onFullSync(long masterRdbOffset) {

    }

    @Override
    public void reFullSync() {

    }

    @Override
    public void beginWriteRdb(EofType eofType, long masterRdbOffset) throws IOException {

    }

    @Override
    public void endWriteRdb() {
        online.set(true);
    }

    @Override
    public void onContinue(String requestReplId, String responseReplId) {
        online.set(true);
    }

    public SettableFuture<Boolean> getOnline() {
        return online;
    }
}
