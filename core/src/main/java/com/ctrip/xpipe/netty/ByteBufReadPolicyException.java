package com.ctrip.xpipe.netty;

import com.ctrip.xpipe.exception.XpipeException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 27, 2017
 */
public class ByteBufReadPolicyException extends XpipeException{

    public ByteBufReadPolicyException(String message){
        super(message);
    }

    public ByteBufReadPolicyException(String message, Throwable th) {
        super(message, th);
    }
}
