package com.ctrip.xpipe.api.migration;

import com.ctrip.xpipe.exception.XpipeException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 26, 2017
 */
public class OuterClientException extends XpipeException{

    public OuterClientException(String message) {
        super(message);
    }

    public OuterClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
