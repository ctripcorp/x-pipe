package com.ctrip.xpipe.metric;

import com.ctrip.xpipe.exception.XpipeException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 27, 2017
 */
public class MetricProxyException extends XpipeException {

    public MetricProxyException(String message) {
        super(message);
    }

    public MetricProxyException(String message, Throwable th) {
        super(message, th);
    }
}
