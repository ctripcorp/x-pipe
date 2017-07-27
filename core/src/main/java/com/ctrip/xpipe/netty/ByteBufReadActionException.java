package com.ctrip.xpipe.netty;

import com.ctrip.xpipe.exception.XpipeException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 27, 2017
 */
public class ByteBufReadActionException extends XpipeException{

    public ByteBufReadActionException(String message){
        super(message);
    }

    public ByteBufReadActionException(String message, Throwable th) {
        super(message, th);
    }
}
