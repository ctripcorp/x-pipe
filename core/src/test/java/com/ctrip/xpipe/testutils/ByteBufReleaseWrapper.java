package com.ctrip.xpipe.testutils;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *         <p>
 *         May 18, 2017
 */
public class ByteBufReleaseWrapper extends AbstractLifecycle{

    private ByteBuf byteBuf;

    public ByteBufReleaseWrapper(ByteBuf byteBuf){

        this.byteBuf = byteBuf;
        try {
            initialize();
        } catch (Exception e) {
            throw new IllegalStateException("initialize", e);
        }
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
    }

    @Override
    protected void doDispose() throws Exception {

        int refCnt = byteBuf.refCnt();

        if(refCnt > 0){
            byteBuf.release(refCnt);
        }
        super.doDispose();
    }
}
